import logging,time,json,os
from pathlib import Path
from sqlalchemy import select
from .database import Session
from .models import SchoolAnalysis,Transect,Echo,BlockEvaluation,BlockEvaluationLog
from .school_service import queue
from .algorithm import analyze,block_outcome,summarize
from .common import dump
from .evaluation_service import completed,staleness
logging.basicConfig(level=logging.INFO)
ARTIFACTS=Path(os.getenv('ARTIFACTS_DIR','/artifacts'))

def write_artifact(name,payload):
    try:
        ARTIFACTS.mkdir(parents=True,exist_ok=True);(ARTIFACTS/name).write_text(json.dumps(payload,ensure_ascii=False))
    except OSError:logging.exception('artifact write failed')

def expire(row,reason):
    row.status='stale';row.error=reason[:240]

def run_evaluation(id):
    with Session() as db:
        row=db.get(BlockEvaluation,id)
        if not row or row.status not in ('queued','running'):return
        row.status='running';db.commit()
        for index in range(row.total_blocks):
            db.refresh(row)
            if row.status!='running':return
            logged=set(db.scalars(select(BlockEvaluationLog.block_index).where(BlockEvaluationLog.evaluation_id==row.id,BlockEvaluationLog.round==row.round)).all())
            if index in logged or any(r['block_index']==index for r in (row.results or [])):continue
            reasons=staleness(db,row)
            if reasons:
                expire(row,'评估中途过期：'+'；'.join(reasons));db.commit();return
            try:
                t=db.get(Transect,row.transect_id)
                analysis=db.get(SchoolAnalysis,row.analysis_id)
                points=[dump(p) for p in db.scalars(select(Echo).where(Echo.transect_id==t.id).order_by(Echo.id))]
                origin=points[0] if points else None
                result=block_outcome(points,analysis.assignments,row.eps,row.min_samples,index,row.block_m,row.total_blocks,origin,t.length_m)
                row.results=[r for r in (row.results or []) if r['block_index']!=index]+[result]
                db.add(BlockEvaluationLog(evaluation_id=row.id,round=row.round,block_index=index,block_start_m=result['start_m'],block_end_m=result['end_m'],result=result))
                total=row.total_blocks
                row.summary=summarize(sorted(row.results,key=lambda r:r['block_index']),total)
                if len(row.results)==total:row.status='done'
                db.commit()
                write_artifact(f'block-eval-{row.id}.json',row.summary)
            except Exception as e:
                logging.exception('block evaluation')
                db.rollback();row=db.get(BlockEvaluation,id)
                row.status='failed';row.error=str(e)[:240];db.commit();return
        db.refresh(row)
        if row.status=='running' and completed(row)==row.total_blocks:
            row.status='done';db.commit()

def run_school(item):
    with Session() as db:
        row=db.get(SchoolAnalysis,int(item))
        if not row or row.status!='queued':return
        row.status='running';db.commit()
        try:
            t=db.get(Transect,row.transect_id)
            if row.data_version!=t.data_version:raise ValueError('回波已更新，请重新提交最新版本')
            points=[dump(p) for p in db.scalars(select(Echo).where(Echo.transect_id==t.id).order_by(Echo.id))]
            row.assignments,row.metrics=analyze(points,row.eps,row.min_samples);row.status='done'
            write_artifact(f'school-{row.id}.json',row.metrics)
        except Exception as e:row.status='failed';row.error=str(e)[:200]
        db.commit()

def loop():
    while True:
        Path('/tmp/worker-heartbeat').touch()
        try:
            item=queue.blpop(['estuary:schools','estuary:blocks'],timeout=2)
            if not item:continue
            kind,value=item
            if kind=='estuary:schools':run_school(value)
            else:run_evaluation(int(value))
        except Exception:logging.exception('echo worker');time.sleep(1)

if __name__=='__main__':loop()
