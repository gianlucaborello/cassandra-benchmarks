import cassandra
from cassandra.cluster import Cluster
import random
import time

COLUMN_SIZE = 100 * 1024
COLUMN_INCREMENT = 10
MAX_COLUMNS = 100
NUM_TIMESTAMPS = 100
IDLE_TIME = 10

cluster = Cluster(["127.0.0.1"])
session = cluster.connect()
session.execute("DROP KEYSPACE IF EXISTS benchmarks");
session.execute("CREATE KEYSPACE benchmarks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
session.execute("CREATE TABLE benchmarks.test (stream_id int, column_no int, timestamp int, column blob, primary key (stream_id, column_no, timestamp));")

blob = bytearray(random.getrandbits(8) for _ in xrange(COLUMN_SIZE))

columns = 0

print("Response time for querying a single column on a large table (column size %d KB):" % (COLUMN_SIZE / 1024))

while columns < MAX_COLUMNS:
	for j in range(COLUMN_INCREMENT):
		for k in range(NUM_TIMESTAMPS):
			session.execute("INSERT INTO benchmarks.test (stream_id, column_no, timestamp, column) VALUES (%s, %s, %s, %s)", (1, columns + j, k, blob))

	columns += COLUMN_INCREMENT

	time.sleep(IDLE_TIME)

	t1 = time.time() * 1000
	rs = session.execute("SELECT * from benchmarks.test where stream_id=1 and column_no=7")
	t2 = time.time() * 1000

	print("%d columns: %d ms" % (columns, t2 - t1))

cluster.shutdown()
