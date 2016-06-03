import redis

from despydb import DesDbi

# Setting up redis object
r = redis.StrictRedis(host='localhost', port=6379, db=0)

# Setting up db cursor
dbh = DesDbi(None,'db-desoper')
cur = dbh.cursor()

query = "select distinct expnum from exposuretag where tag = 'Y2A1'"
cur.execute(query)
expnums = [f[0] for f in cur.fetchall()]

for e in expnums: r.sadd('y3a1f',e)

print r.smembers('y3a1f')
