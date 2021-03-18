Trigger Deployment Steps

1. Package trigger jar with all the dependencies.
2. Copy the jar file to /etc/cassandra/conf/triggers/ directory on all the cassandra nodes.
3. Create a properties file with name trigger.properties and give the below properties in it.
      topic = <topic_name>
      bootstrap.servers = <kafka-broker:port> (eg: 10.105.22.175:9092)
Put this property file in the same directory as jar i.e /etc/cassandra/conf/triggers/  on all the nodes.
4. After placing the jar, run below command on each node.
      nodetool reloadtriggers
5. You can check the logs in /var/log/cassandra/system.log for the jar has been loaded.
6. Now open cqlsh on any node and use the below command to create trigger.
      create trigger <TRIGGER_NAME> on <keyspace>.<tablename> USING 'com.trigger.Trigger';
7. Check the logs again in /var/log/cassandra/system.log to verify trigger has been created successfully.
8. You can also check the existing triggers using the below command.
      Select * from system_schema.triggers;
9. To delete any trigger use the below command.
      Drop trigger TRIGGER_NAME on Keyspace.TableName;
