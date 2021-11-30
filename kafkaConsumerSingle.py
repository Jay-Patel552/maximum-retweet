from kafka import KafkaConsumer
import json
import sys

if __name__=='__main__':
    N_covid=(int(sys.argv[1]))//2
    N_vaccine = (int(sys.argv[1])) // 2
    max_count_covid=0
    max_count_vaccine=0
    consumer = KafkaConsumer(bootstrap_servers='localhost:9092', auto_offset_reset='earliest',
                             consumer_timeout_ms=1000, group_id='my_grp',value_deserializer=lambda x: json.loads(x))
    consumer.subscribe(topics=['covid-tweets','vaccine-tweets'],pattern=None,listener=None)
    previous_ID_covid=0
    previous_ID_vaccine=0
    Max_retweet_obj_covid = None
    Max_retweet_obj_vaccine = None
    while True:
        msg_poll=consumer.poll(timeout_ms=1000)
        #print("LLLLLLL")
        for msg in msg_poll.items():
            #print(msg)
            for msg1 in msg[1]:
                #print(msg1)
                pyobj1 = msg1.value
                if pyobj1.get('matching_rules')[0].get("tag") =="covid123":
                    N_covid-=1
                    if N_covid<0:
                        pass
                    else:
                        if (previous_ID_covid)!=pyobj1.get('ID')-1:
                            print('anamoly in sequence ID')
                        previous_ID_covid=pyobj1.get('ID')
                        if max_count_covid <= pyobj1.get('data').get('public_metrics').get('retweet_count'):
                            max_count_covid = pyobj1.get('data').get('public_metrics').get('retweet_count')
                            Max_retweet_obj_covid="ID: " + str(pyobj1.get('ID')) + ' Tweet with retweet count ' + str(
                                pyobj1.get('data').get('public_metrics').get(
                                    'retweet_count')) + ' is received from the topic:covid-tweets\n' + pyobj1.get('data').get(
                                'text') + '\n'
                            print(Max_retweet_obj_covid)
                else:
                    N_vaccine-=1
                    if N_vaccine<0:
                        pass
                    else:
                        if previous_ID_vaccine!=pyobj1.get('ID')-1:
                            print('anamoly in sequence ID')
                        previous_ID_vaccine=pyobj1.get('ID')
                        if max_count_vaccine <= pyobj1.get('data').get('public_metrics').get('retweet_count'):
                            max_count_vaccine = pyobj1.get('data').get('public_metrics').get('retweet_count')
                            Max_retweet_obj_vaccine="ID: " + str(pyobj1.get('ID')) + ' Tweet with retweet count ' + str(
                                pyobj1.get('data').get('public_metrics').get(
                                    'retweet_count')) + ' is received from the topic:vaccine-tweets\n' + pyobj1.get('data').get(
                                'text') + '\n'
                            print(Max_retweet_obj_vaccine)
                #print(N_covid)
                #print(N_vaccine)
                if N_vaccine<=0 and N_covid<=0:
                    break
        if N_vaccine<=0 and N_covid<=0:
            break
    print(Max_retweet_obj_covid)
    print(Max_retweet_obj_vaccine)
