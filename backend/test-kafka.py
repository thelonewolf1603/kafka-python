from kafka.admin import KafkaAdminClient, NewTopic

try:
    admin = KafkaAdminClient(
        bootstrap_servers=['localhost:9092'],
        client_id='test'
    )
    
    # Create a test topic
    topic = NewTopic(name='test-topic', num_partitions=1, replication_factor=1)
    admin.create_topics([topic])
    
    print("✅ Successfully connected to Kafka!")
    print("Topics:", admin.list_topics())
    admin.close()
    
except Exception as e:
    print(f"❌ Connection failed: {e}")