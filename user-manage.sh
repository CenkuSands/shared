Logging in to Confluent MDS...
Username: admin
Password: ******
Select the cluster type to assign RBAC roles for:
1) Kafka
2) Schema Registry
3) Connect
4) ksqlDB
5) Quit
Choose a cluster: 1
Select the resource type for Kafka:
1) Cluster
2) Topic
3) ConsumerGroup
4) TransactionalId
Choose a resource type: 2
Enter the resource name (e.g., my-topic, jdbc-sink, or 'kafka-cluster' for Cluster): my-topic
Select the role to assign:
1) SystemAdmin
2) SecurityAdmin
3) ClusterAdmin
4) Operator
5) ResourceOwner
6) DeveloperRead
7) DeveloperWrite
8) DeveloperManage
Choose a role: 6
Enter the user principal (e.g., User:alice or ServiceAccount:12345): User:alice
About to assign the following RBAC role:
Cluster: Kafka (KQey0SYmQ_uT6Vcq-0y9gA)
Resource Type: Topic
Resource Name: my-topic
Role: DeveloperRead
Principal: User:alice
Confirm assignment? (y/n): y
Executing: confluent iam rbac role-binding create --principal User:alice --role DeveloperRead --resource Topic:my-topic --kafka-cluster-id KQey0SYmQ_uT6Vcq-0y9gA
Role assignment successful!
