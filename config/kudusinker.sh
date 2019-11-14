git clone --branch v1.3.1 https://github.com/hpgrahsl/kafka-connect-mongodb.git

cp ~/confluent/bin/kafka-run-class ~/confluent/bin/kafka-connect-rd
cp ~/confluent/bin/connect-distributed ~/confluent/bin/connect-distributed-rd

ansible -i /etc/ansible/hosts-hadoop all -m shell -a"mkdir ~/confluent/share/java/kafka-connect-contrib"

#local
scp -i fm_beta_bigdata.pem /mnt/btu/libcdcdw/target/libcdcdw-1.0.0-SNAPSHOT.jar root@beta-hbase01:/var/lib/hadoop-hdfs/confluent/share/java/kafka-connect-contrib/
ssh -i fm_beta_bigdata.pem root@beta-hbase01 chown hdfs:hdfs /var/lib/hadoop-hdfs/confluent/share/java/kafka-connect-contrib/libcdcdw-1.0.0-SNAPSHOT.jar
scp -i fm_beta_bigdata.pem /mnt/btu/kafka-connect-contrib/target/kafka-connect-contrib-1.3.1-SNAPSHOT.jar root@beta-hbase01:/var/lib/hadoop-hdfs/confluent/share/java/kafka-connect-contrib/
ssh -i fm_beta_bigdata.pem root@beta-hbase01 chown hdfs:hdfs /var/lib/hadoop-hdfs/confluent/share/java/kafka-connect-contrib/kafka-connect-contrib-1.3.1-SNAPSHOT.jar

ansible -i /etc/ansible/hosts-hadoop slave -m copy -a"src=~/confluent/share/java/kafka-connect-contrib/kafka-connect-contrib-1.3.1-SNAPSHOT.jar dest=~/confluent/share/java/kafka-connect-contrib"
ansible -i /etc/ansible/hosts-hadoop slave -m copy -a"src=~/confluent/share/java/kafka-connect-contrib/libcdcdw-1.0.0-SNAPSHOT.jar dest=~/confluent/share/java/kafka-connect-contrib"

#bson-3.10.1.jar
#kudu-client-1.10.0.jar
#kudu-test-utils-1.10.0.jar
#vavr-0.9.2.jar
