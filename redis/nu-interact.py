from datetime import datetime
import pandas

import redis
from numba import jit

def connect_to_redis(host='localhost',port=6379,db=0):
    # Setting up redis object
    r = redis.StrictRedis(host=host, port=port, db=db)
    return r

def connect_to_db(section='db-desoper'):
    # Setting up db cursor
    dbh = DesDbi(None,section)
    cur = dbh.cursor()
    return cur

def add_campaign(tag):
    query = "select distinct expnum from exposuretag where tag = '{tag}".format(tag=tag)
    cur.execute(query)
    expnums = [f[0] for f in cur.fetchall()]
    for e in expnums: r.sadd(tag,e)

def check_passed(cur,reqnum,unitname,attnum):
    # Returns (exists,passed)
    query = "select distinct status from task t,pfw_attempt a where t.id=a.task_id and a.unitname='{unitname}' and a.reqnum={reqnum} and a.attnum={attnum}".format(reqnum=reqnum,unitname=unitname,attnum=attnum)
    cur.execute(query)

    status = cur.fetchone()[0]

    if status == 0:
        return (status,True)
    elif status != 0:
        return (status,False)
def return_tagged_runs(cur,tag):
    query = "select distinct reqnum,unitname,attnum from proctag where tag= '{tag}' order by reqnum,unitname,attnum".format(tag=tag)
    cur.execute(query)
    return pandas.DataFrame(cur.fetchall(),columns=['reqnum','unitname','attnum'])

@jit
def update_queue(df):
    for i,row in runs.iterrows():
        expnum = row.unitname.split('D00')[1]
        #status,passed = check_passed(cur,row.reqnum,row.unitname,row.attnum)
        if row.status ==0:
            r.sadd(success,expnum)
            r.srem(tag,expnum)
        else:
            r.sadd(tag,expnum)    

if __name__=='__main__':

    start =  datetime.now()
    print(start)
    #cur = connect_to_db()
    r = connect_to_redis()
    tag = 'Y3A1_FINALCUT'
    success = tag + '_PASSED'   
    
    #runs = return_tagged_runs(cur,tag)
    runs = pandas.read_csv('y3a1f.csv')
    print('Dataframe created...')
    update_queue(runs)
 
    print('Orig: {o}'.format(o=r.scard('Y3A1_FINALCUT')))
    print('Passed: {p}'.format(p=r.scard('Y3A1_FINALCUT_PASSED')))
    end = datetime.now()
    print(end - start)

