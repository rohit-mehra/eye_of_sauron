# set broker properties, using pegasus the following commands, else do it manually
# make sure that settings here correspond to those in params.py

peg sshcmd-cluster kfc "sudo sed -i '/num.network.threads=/c\num.network.threads=3' /usr/local/kafka/config/server.properties"
peg sshcmd-cluster kfc "sudo sed -i '/num.io.threads=/c\num.io.threads=6' /usr/local/kafka/config/server.properties"
peg sshcmd-cluster kfc "sudo sed -i '/log.retention.hours=/c\log.retention.hours=3' /usr/local/kafka/config/server.properties"
peg sshcmd-cluster kfc "sudo sed -i '/log.retention.bytes=/c\log.retention.bytes=10000000000' /usr/local/kafka/config/server.properties"
peg sshcmd-cluster kfc "sudo sed -i '/group.initial.rebalance.delay.ms=/c\group.initial.rebalance.delay.ms=10' /usr/local/kafka/config/server.properties"
# set less than 100 * #brokers * replication factor
peg sshcmd-cluster kfc "sudo sed -i '/num.partitions=/c\num.partitions=16' /usr/local/kafka/config/server.properties"