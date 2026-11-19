import math,hashlib
import numpy as np
from sklearn.cluster import DBSCAN
from sklearn.model_selection import train_test_split
from sklearn.metrics import adjusted_rand_score

def features(points,origin=None):
    o=origin or points[0];lon0=o['lon'];lat0=o['lat']
    return np.array([[math.radians(p['lon']-lon0)*6371000*math.cos(math.radians(lat0))/60,math.radians(p['lat']-lat0)*6371000/60,p['depth_m']/3,p['strength_db']/8] for p in points])

def analyze(points,eps,min_samples):
    x=features(points)
    labels=DBSCAN(eps=eps,min_samples=min_samples).fit_predict(x)
    rng=np.random.default_rng(37);ref=np.vstack([rng.normal(c,.16,(35,4)) for c in [[0,0,2,-5],[4,0,5,-4],[8,0,8,-7]]]+[rng.uniform([-1,-1,0,-12],[10,1,10,-2],(15,4))]);truth=np.repeat([0,1,2,-1],[35,35,35,15])
    train,test=train_test_split(np.arange(120),test_size=.3,random_state=37,stratify=truth)
    sensitivity=[]
    for e in [.8,1.2,1.6]:
        observed=DBSCAN(eps=e,min_samples=min_samples).fit_predict(x);held=DBSCAN(eps=e,min_samples=3).fit_predict(ref[test]);sensitivity.append({'eps':e,'clusters':len(set(observed)-{-1}),'noise':int(sum(observed==-1)),'reference_holdout_ari':float(adjusted_rand_score(truth[test],held))})
    hold=DBSCAN(eps=eps,min_samples=3).fit_predict(ref[test])
    return [{'echo_id':p['id'],'label':int(label)} for p,label in zip(points,labels)],{'clusters':len(set(labels)-{-1}),'noise_count':int(sum(labels==-1)),'point_count':len(points),'reference_holdout_ari':float(adjusted_rand_score(truth[test],hold)),'sensitivity':sensitivity,'reference_train_count':len(train),'reference_holdout_count':len(test),'seed':37,'input_sha256':hashlib.sha256(x.tobytes()).hexdigest(),'source':'实际回波聚类；ARI为独立合成参照的36点留出验证，无监督算法不训练预测模型'}

def block_index(point,block_m,total_blocks):
    return min(int(point['along_m']//block_m),total_blocks-1)

def block_outcome(points,assignments,eps,min_samples,index,block_m,total_blocks,origin=None,length_m=None):
    '''留一块重算：移除指定百米块后用原 eps/min_samples 重新聚类，只凭仍保留的共同回波追踪原群去向。'''
    original={a['echo_id']:int(a['label']) for a in assignments}
    survivors=[p for p in points if block_index(p,block_m,total_blocks)!=index]
    removed={p['id'] for p in points if block_index(p,block_m,total_blocks)==index}
    new_label={}
    if survivors:
        labels=DBSCAN(eps=eps,min_samples=min_samples).fit_predict(features(survivors,origin))
        new_label={p['id']:int(label) for p,label in zip(survivors,labels)}
    clusters=[]
    for label in sorted({v for v in original.values() if v>=0}):
        members=[k for k,v in original.items() if v==label]
        kept=[k for k in members if k not in removed]
        labels=[new_label[k] for k in kept if new_label.get(k,-1)>=0]
        noise=sum(1 for k in kept if new_label.get(k,-1)==-1)
        distinct=sorted(set(labels))
        if not kept:outcome='no_comparable'
        elif not distinct:outcome='noise'
        elif len(distinct)>1:outcome='split'
        else:outcome='retained'
        clusters.append({'label':label,'total':len(members),'removed':len(members)-len(kept),'comparable':len(kept),'new_labels':distinct,'noise_in_comparable':noise,'outcome':outcome})
    end_m=min((index+1)*block_m,length_m if length_m is not None else total_blocks*block_m)
    return {'block_index':index,'start_m':index*block_m,'end_m':end_m,'removed_count':len(removed),'removed_labels':sorted({original[k] for k in removed if original.get(k,-1)>=0}),'new_cluster_count':len(set(new_label.values())-{-1}),'new_noise_count':sum(1 for v in new_label.values() if v==-1),'clusters':clusters}

def summarize(results,total_blocks):
    per={}
    for r in results:
        for c in r['clusters']:
            e=per.setdefault(c['label'],{'label':c['label'],'blocks':0,'retained':0,'split':0,'noise':0,'no_comparable':0,'sensitive_blocks':[]})
            e['blocks']+=1;e[c['outcome']]+=1
            if c['outcome'] in ('split','noise'):e['sensitive_blocks'].append(r['block_index'])
    for e in per.values():e['sensitive_fraction']=round((e['split']+e['noise'])/e['blocks'],4) if e['blocks'] else 0
    return {'total_blocks':total_blocks,'completed_blocks':len(results),'blocks_with_split':sum(1 for r in results if any(c['outcome']=='split' for c in r['clusters'])),'blocks_with_noise':sum(1 for r in results if any(c['outcome']=='noise' for c in r['clusters'])),'blocks_with_no_comparable':sum(1 for r in results if any(c['outcome']=='no_comparable' for c in r['clusters'])),'clusters':sorted(per.values(),key=lambda e:e['label'])}
