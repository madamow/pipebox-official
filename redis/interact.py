import redis

from despydb import DesDbi

def connect_to_redis(host='localhost',port=6379,db=0)
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
    query = "select distinct status from task t,pfw_attempt a where t.id=a.task_id and a.unitname='{unitname}' and a.reqnum={reqnum} and a.attnum={attnum}".format(reqnum=reqnum,unitname=unitname,attnum=attnum)
    cur.execute(query)

    status = cur.fetchone()

    if status == 0:
        return True
    elif status != 0:
        return False
    else:
        return False

def update_queue(red,expnum):
    
    pass

if __name__=='__main__':
    cur = connect_to_db()
    r = connect_to_redis()
    print check_passed(2390,'D00483514',1)
    #print r.smembers('y3a1f')
