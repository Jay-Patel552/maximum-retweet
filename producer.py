from kafka import KafkaProducer
from stream_obtain import twit_stream
import sys
import json
import os
import time

#producer=KafkaProducer(bootstrap_servers='localhost:9092')

if __name__=='__main__':
    id_covid=0
    id_covid_vaccine=0
    batch_size=int(sys.argv[1])//10+1
    req_rules=[{"value":"covid","tag":"covid123"},{"value":'"covid vaccine"',"tag":"vaccine"}]
    req_rules2=[{"value":'"covid vaccine"',"tag":"vaccine"}]
    req_rules1=[{"value":"covid","tag":"covid123"}]
    ls_covid=[]
    ls_vaccine=[]
    while(1):
        temp_ls=[]
        if id_covid <= int(sys.argv[1]) and id_covid_vaccine<= int(sys.argv[1]):
            json_response_list=twit_stream(req_rules,temp_ls,batch_size)
            for json_response in json_response_list:
                matching_rule=json_response.get("matching_rules")[0].get('tag')
                if matching_rule=='covid123':
                    id_covid+=1
                    json_response.update({'ID':id_covid})
                    ls_covid.append(json.dumps(json_response,sort_keys=True))
                if matching_rule=='vaccine':
                    id_covid_vaccine+=1
                    json_response.update({'ID':id_covid_vaccine})
                    ls_vaccine.append(json.dumps(json_response,sort_keys=True))
        elif id_covid <= int(sys.argv[1]) and id_covid_vaccine > int(sys.argv[1]):
            json_response_list=twit_stream(req_rules1,temp_ls,batch_size)
            for json_response in json_response_list:
                id_covid+=1
                json_response.update({'ID':id_covid})
                ls_covid.append(json.dumps(json_response,sort_keys=True))
        elif id_covid > int(sys.argv[1]) and id_covid_vaccine <= int(sys.argv[1]):
            json_response_list=twit_stream(req_rules2,temp_ls,batch_size)
            for json_response in json_response_list:
                id_covid_vaccine+=1
                json_response.update({"ID":id_covid_vaccine})
                ls_vaccine.append(json.dumps(json_response, sort_keys=True))
        else:
            break
        time.sleep(0.34)
    print(ls_covid)
    print(ls_vaccine)