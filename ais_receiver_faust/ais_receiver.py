
import os
import socket
import ais.stream
import json
import time
from datetime import datetime 
from kafka import KafkaProducer, KafkaClient
from kafka.admin import KafkaAdminClient, NewTopic

KYSTINFO_HOST = os.getenv('HOST', '10.222.173.168')
KYSTINFO_PORT = int(os.getenv('PORT', 4712))

KAFKAHOST = os.getenv('KAFKAHOST', 'localhost')
KAFKAPORT = os.getenv('KAFKAPORT', '9092')

producer = KafkaProducer(bootstrap_servers=f'{KAFKAHOST}:{KAFKAPORT}')
producer = KafkaProducer(bootstrap_servers=f'{KAFKAHOST}:{KAFKAPORT}',
                         value_serializer=lambda m: json.dumps(m).encode('utf8'))

print (KYSTINFO_HOST, KYSTINFO_PORT)
if __name__ == '__main__':
    while True:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(120)
            print('connect to socket')   
            s.connect((KYSTINFO_HOST, KYSTINFO_PORT))
            print('make file')   

            f = s.makefile()
            store = {}

            i=0
            msg_list = []
            start = time.time()
            for msg in ais.stream.decode(f):
                if msg['id'] not in [1, 2, 3]:
                    print(msg)
                if msg['id'] in [1, 2, 3]:
                    producer.send('ais', msg)
                    i+=1 
                elif msg['id'] in [19, 21]:
                    producer.send('shipdetail', msg)
        except Exception as e:

            kafka_client = KafkaClient(f'{KAFKAHOST}:{KAFKAPORT}')
            server_topics = kafka_client.topic_partitions
            print(server_topics)
            admin_client = KafkaAdminClient(
                bootstrap_servers=f'{KAFKAHOST}:{KAFKAPORT}', 
                #client_id='test'
            )

            topic_list = []
            for topic_name in ['ais', 'shipdetail']:
                if topic_name not in server_topics:
                    
                    topic_list.append(NewTopic(name=topic_name, num_partitions=1, replication_factor=1))
            admin_client.create_topics(new_topics=topic_list, validate_only=False)

                
            print(e)
            time.sleep(30)
