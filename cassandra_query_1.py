import cassandra
from cassandra.cluster import Cluster
import time

cluster = Cluster(["127.0.0.1"])
session = cluster.connect()

t1 = time.time() * 1000
rs = session.execute("SELECT column7 from benchmarks.test")
t2 = time.time() * 1000

print("%d ms" % (t2 - t1))

cluster.shutdown()
