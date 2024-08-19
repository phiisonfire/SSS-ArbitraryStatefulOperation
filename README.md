kafka cli

# Create new topic
kafka-topics --create --bootstrap-server localhost:9092 \
--replication-factor 1 \
--partitions 1 \
--topic user-transaction-topic

# Delete topic
kafka-topics --bootstrap-server localhost:9092 \
--delete \
--topic user-transaction-topic

# Consumer groups
kafka-consumer-groups --bootstrap-server localhost:9092 --list
kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group <consumer-group-id>

# Console-consumer
kafka-console-consumer \
--bootstrap-server localhost:9092 \
--topic user-transaction-topic \
--from-beginning

# add this VM option to the Run Configuration of Intellij when using JDK17
--add-exports java.base/sun.nio.ch=ALL-UNNAMED

Cassandra
# start interacting with cassandra cqlsh
docker exec -it cassandra cqlsh

# create keyspace
CREATE KEYSPACE IF NOT EXISTS streaming_demo WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};

# create table
CREATE TABLE IF NOT EXISTS streaming_demo.user_transaction_counts (
"userId" TEXT PRIMARY KEY,
"purchaseCount" INT,
"eventTimestamp" TIMESTAMP,
"isTimeout" BOOLEAN
);

# drop table
DROP TABLE IF EXISTS streaming_demo.user_transaction_counts;

