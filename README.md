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

