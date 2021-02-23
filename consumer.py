from kafka import KafkaConsumer
import json
import os
def covid_consumer():
    ls_covid=[]
    max_count=1
    consumer=KafkaConsumer('covid-tweets',bootstrap_servers='localhost:9092',auto_offset_reset='earliest',consumer_timeout_ms=1000,value_deserializer=lambda x: json.loads(x))
    for msg in consumer:
        #print(msg)
        pyobj=msg.value
        if max_count<=pyobj.get('data').get('public_metrics').get('retweet_count'):
            max_count=pyobj.get('data').get('public_metrics').get('retweet_count')
            print("ID: "+str(pyobj.get('ID'))+' Tweet with retweet count '+ str(pyobj.get('data').get('public_metrics').get('retweet_count'))+' is received from the topic:covid-tweets\n'+ pyobj.get('data').get('text')+'\n')
def vaccine_consumer():
    max_count=1
    consumer=KafkaConsumer('vaccine-tweets',bootstrap_servers='localhost:9092',auto_offset_reset='earliest',consumer_timeout_ms=1000,value_deserializer=lambda x: json.loads(x))
    for msg in consumer:
        #print(msg)
        pyobj=msg.value
        if max_count<=pyobj.get('data').get('public_metrics').get('retweet_count'):
            max_count=pyobj.get('data').get('public_metrics').get('retweet_count')
            print("ID: "+str(pyobj.get('ID'))+' Tweet with retweet count '+str(pyobj.get('data').get('public_metrics').get('retweet_count'))+' is received from the topic:vaccine-tweets\n'+ pyobj.get('data').get('text')+"\n")

if __name__=='__main__':
    pid=os.fork()
    if pid==0:
        covid_consumer()
    else:
        vaccine_consumer()