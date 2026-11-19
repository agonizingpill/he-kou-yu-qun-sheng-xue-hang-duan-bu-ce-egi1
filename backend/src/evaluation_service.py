import hashlib,json,math,redis
from datetime import datetime,timezone
from fastapi import HTTPException
from sqlalchemy import select
from .models import SchoolAnalysis,SchoolReview,Transect,BlockEvaluation,BlockEvaluationLog
from .common import get,reject,finish,transect_lock,dump
from .school_service import latest,queue

def blocks(length_m,block_m=100):
    return max(1,math.ceil(length_m/block_m))

def completed(row):
    return len({r['block_index'] for r in (row.results or [])})

def _source_sha(analysis,review):
    payload={'analysis_id':analysis.id,'data_version':analysis.data_version,'eps':analysis.eps,'min_samples':analysis.min_samples,'assignments':analysis.assignments,'review_id':review.id,'review_decision':review.decision}
    return hashlib.sha256(json.dumps(payload,sort_keys=True,ensure_ascii=False,separators=(',',':')).encode('utf-8')).hexdigest()

def _push(db,row):
    db.flush()
    try:queue.rpush('estuary:blocks',row.id)
    except redis.RedisError:
        row.status='failed';row.error='队列暂不可用，恢复后可续算剩余块';db.commit();raise HTTPException(503,row.error)

def create(db,analysis_id,user):
    analysis=get(db,SchoolAnalysis,analysis_id)
    c,t=transect_lock(db,analysis.transect_id)
    official=latest(db,t.id,True)
    reject(not official or official.id!=analysis.id,'仅最新回波版本的已复核识别可做分块评估')
    review=db.scalar(select(SchoolReview).where(SchoolReview.analysis_id==analysis.id))
    reject(not review,'识别尚未复核，先完成群落与噪点复核')
    total=blocks(t.length_m)
    row=db.scalar(select(BlockEvaluation).where(BlockEvaluation.analysis_id==analysis.id).order_by(BlockEvaluation.id.desc()))
    sha=_source_sha(analysis,review)
    if row is None:
        row=BlockEvaluation(analysis_id=analysis.id,transect_id=t.id,data_version=t.data_version,eps=analysis.eps,min_samples=analysis.min_samples,block_m=100,total_blocks=total,source_sha256=sha,source_review_id=review.id,results=[],summary={},status='queued',round=1)
        db.add(row);db.flush()
    else:
        reject(row.status=='done' and completed(row)==total and row.source_sha256==sha,'该识别的百米留一评估已算齐；补录回波或换正式识别后按新版本重新提交')
        reject(row.status in ('queued','running'),'该识别的分块评估正在计算，请稍后刷新')
        if row.source_sha256!=sha or row.total_blocks!=total:
            row.results=[];row.summary={};row.total_blocks=total;row.data_version=t.data_version;row.eps=analysis.eps;row.min_samples=analysis.min_samples
            row.source_review_id=review.id
        row.source_sha256=sha
        row.status='queued';row.error='';row.round=(row.round or 1)+1;db.flush()
    result=finish(db,user,'提交百米留一分块评估',row);_push(db,row);return result

def resume(db,id,user):
    initial=get(db,BlockEvaluation,id);c,t=transect_lock(db,initial.transect_id)
    row=get(db,BlockEvaluation,id,True)
    reject(row.status=='done' and completed(row)==row.total_blocks,'该评估各块已算齐，无需续算')
    reasons=staleness(db,row)
    reject(bool(reasons),'评估已过期，请按最新回波与正式识别重新提交：'+('；'.join(reasons)))
    last=db.scalar(select(BlockEvaluationLog.at).where(BlockEvaluationLog.evaluation_id==row.id).order_by(BlockEvaluationLog.id.desc()))
    if row.status in ('queued','running') and last and (datetime.now(timezone.utc)-last).total_seconds()<60:
        reject(True,'评估正在计算，请稍后刷新')
    row.status='queued';row.error='';row.round=(row.round or 1)+1;db.flush()
    result=finish(db,user,'续算百米留一剩余块',row);_push(db,row);return result

def staleness(db,row):
    reasons=[]
    t=db.get(Transect,row.transect_id)
    if not t or t.status=='cancelled':return ['航段已撤销']
    if t.data_version!=row.data_version:reasons.append('补录回波后航段回波版本已更新')
    official=latest(db,t.id,True)
    if not official or official.id!=row.analysis_id:reasons.append('已有新的正式识别复核结果')
    elif row.source_sha256!=_source_sha(official,db.scalar(select(SchoolReview).where(SchoolReview.analysis_id==official.id))):reasons.append('对应识别或复核内容已变化')
    if (row.total_blocks or 0)!=blocks(t.length_m):reasons.append('航段长度变化，分块需要重排')
    return reasons

def current(db,row):
    return bool(row and row.status=='done' and completed(row)==row.total_blocks and not staleness(db,row))

def stale_except(db,transect_id,analysis_id):
    for row in db.scalars(select(BlockEvaluation).where(BlockEvaluation.transect_id==transect_id,BlockEvaluation.analysis_id!=analysis_id,BlockEvaluation.status!='stale')):
        row.status='stale';row.error='已签发新的正式识别，旧评估过期，需重跑';db.flush()

def stale_for_transect(db,transect_id):
    for row in db.scalars(select(BlockEvaluation).where(BlockEvaluation.transect_id==transect_id,BlockEvaluation.status!='stale')):
        row.status='stale';row.error='补录回波后版本更新，评估过期，需按最新回波与识别重跑';db.flush()

def logs(db,id):
    return [{'id':v.id,'round':v.round,'block_index':v.block_index,'start_m':v.block_start_m,'end_m':v.block_end_m,'result':v.result,'at':v.at} for v in db.scalars(select(BlockEvaluationLog).where(BlockEvaluationLog.evaluation_id==id).order_by(BlockEvaluationLog.round,BlockEvaluationLog.block_index))]

def detail(db,row):
    return {**dump(row),'blocks_log':logs(db,row.id),'stale_reasons':staleness(db,row),'is_current':current(db,row)}
