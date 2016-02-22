import cassandra
from cassandra.cluster import Cluster
import time

cluster = Cluster(["127.0.0.1"])
session = cluster.connect()

t1 = time.time() * 1000
rs = session.execute("SELECT * from benchmarks.test where stream_id=1 and column_no=7")
t2 = time.time() * 1000

print("%d ms" % (t2 - t1))

cluster.shutdown()
