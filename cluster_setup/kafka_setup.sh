# set broker properties, using pegasus the following commands, else do it manually
# make sure that settings here correspond to those in params.py

peg sshcmd-cluster kfc "sudo sed -i '/num.network.threads=/c\num.network.threads=3' /usr/local/kafka/config/server.properties"
peg sshcmd-cluster kfc "sudo sed -i '/num.io.threads=/c\num.io.threads=6' /usr/local/kafka/config/server.properties"
peg sshcmd-cluster kfc "sudo sed -i '/num.partitions=/c\num.partitions=16' /usr/local/kafka/config/server.properties"
peg sshcmd-cluster kfc "sudo sed -i '/log.retention.hours=/c\log.retention.hours=3' /usr/local/kafka/config/server.properties"
peg sshcmd-cluster kfc "sudo sed -i '/log.retention.bytes=/c\log.retention.bytes=10000000000' /usr/local/kafka/config/server.properties"

#peg sshcmd-cluster kfc "echo -e \n >> /usr/local/kafka/config/server.properties"
#peg sshcmd-cluster kfc "echo log.flush.interval.messages=10000 >> /usr/local/kafka/config/server.properties"
#peg sshcmd-cluster kfc "echo delete.topic.enable=true >> /usr/local/kafka/config/server.properties"


#peg sshcmd-cluster kfc 'echo \\n >> /home/ubuntu/.bashrc'
#peg sshcmd-cluster kfc 'echo alias kafka_start="sudo /usr/local/kafka/bin/kafka-server-start.sh /usr/local/kafka/config/server.properties &" >> /home/ubuntu/.bashrc'
#peg sshcmd-cluster kfc 'echo alias kafka_stop="sudo /usr/local/kafka/bin/kafka-server-stop.sh &" >> /home/ubuntu/.bashrc'
