import threading, logging, time
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
from datetime import datetime
 

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
topic = "bigDataAssignment"
filename = "sample_data.json"
message = "Lets get Started_"+ str(datetime.now().strftime("%Y-%m-%dT:%H:%M:%S"))
producer.send(topic, bytes(message, 'utf-8'))
with open (filename, "r") as myfile:
    data = myfile.readlines()
    for line in data:
        print(line)
        producer.send(topic,json.dumps(json.loads(line)).encode('utf-8'))
        time.sleep(5) 
producer.close()