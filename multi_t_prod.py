from kafka import KafkaProducer
#from stream_obtain import twit_stream
import sys
import json
import os
import time
import threading

import requests

stop_stream=0
temp_ls=[]
connection_established=0

def refine_rules(header,req_rules):
    current_rules=requests.get("https://api.twitter.com/2/tweets/search/stream/rules",headers=header)
    #print(json.dumps(current_rules.json()))
    current_rules_json=current_rules.json()

    if current_rules_json is None or "data" not in current_rules_json:
        pass
    else:
        list_ids=list(map(lambda rule: rule["id"], current_rules_json["data"]))
        #print(list_ids)
        to_delete={"delete":{'ids':list_ids}}
        response=requests.post("https://api.twitter.com/2/tweets/search/stream/rules",headers=header,json=to_delete)
        if response.status_code != 200:
            raise Exception(
                "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
                )
            )
    after_delete_rules=requests.get("https://api.twitter.com/2/tweets/search/stream/rules",headers=header)
    #print(json.dumps(after_delete_rules.json()))

    to_add={'add':req_rules}
    new_rules=requests.post("https://api.twitter.com/2/tweets/search/stream/rules",headers=header,json=to_add)
    if new_rules.status_code != 201:
        raise Exception(
            "Cannot add rules (HTTP {}): {}".format(
            new_rules.status_code, new_rules.text
            )
        )
    else:    
        current_rules=requests.get("https://api.twitter.com/2/tweets/search/stream/rules",headers=header)
        print(json.dumps(current_rules.json())) #json.dumps----->python object to json. Serialize obj as a JSON formatted stream to fp (a .write()-supporting file-like object) using this conversion table.

def twit_stream(req_rules):
    global connection_established
    global stop_stream
    global temp_ls
    #print('1')
    bearer_token=os.environ.get("BEARER_TOKEN")
    bearer_str="Bearer "+str(bearer_token)
    header={"Authorization":bearer_str}
    refine_rules(header,req_rules)
    
    params={'tweet.fields':'public_metrics'}
    #params={'tweet.fields':'created_at','tweet.fields':'referenced_tweets','tweet':'retweet_count'}
    

    response=requests.get('https://api.twitter.com/2/tweets/search/stream',params=params,headers=header,stream=True)
    i=0
    for line in response.iter_lines():
        if stop_stream==1:
            response.close()
            print('connection closed')
            return
        else:
            json_response = json.loads(line)  #json.loads----->Deserialize fp (a .read()-supporting text file or binary file containing a JSON document) to a Python object using this conversion table.ie json to python object 
            i+=1
            temp_ls.append(json_response)
            connection_established=1
    return


if __name__ == "__main__":
    id_covid=0
    id_covid_vaccine=0
    req_rules=[{"value":"covid","tag":"covid123"},{"value":'"covid vaccine"',"tag":"vaccine"}]
    req_rules2=[{"value":'"covid vaccine"',"tag":"vaccine"}]
    req_rules1=[{"value":"covid","tag":"covid123"}]
    ls_covid=[]
    ls_vaccine=[]
    while(1):
        if id_covid <= int(sys.argv[1]) and id_covid_vaccine<= int(sys.argv[1]):
            stop_stream=0
            temp_ls=[]
            connection_established=0
            thread_stream=threading.Thread(target=twit_stream,args=(req_rules,))
            thread_stream.start()
            while connection_established==0 :
                time.sleep(0.1)
            for json_response in temp_ls:
                matching_rule=json_response.get("matching_rules")[0].get('tag')
                if matching_rule=='covid123':
                    id_covid+=1
                    json_response.update({'ID':id_covid})
                    json_response.update({'MR':matching_rule})
                    ls_covid.append(json.dumps(json_response,sort_keys=True))
                if matching_rule=='vaccine':
                    id_covid_vaccine+=1
                    json_response.update({'ID':id_covid_vaccine})
                    json_response.update({'MR':matching_rule})
                    ls_vaccine.append(json.dumps(json_response,sort_keys=True))
                if id_covid > int(sys.argv[1]) or id_covid_vaccine > int(sys.argv[1]):
                    stop_stream=1
            print('sleeping')
            time.sleep(5)
            #thread_stream.join()
        elif id_covid <= int(sys.argv[1]) and id_covid_vaccine > int(sys.argv[1]):
            stop_stream=0
            temp_ls.clear()
            connection_established=0
            print("one more")
            time.sleep(5)
            thread_stream1=threading.Thread(target=twit_stream,args=(req_rules1,))
            thread_stream1.start()
            while(connection_established==0):
                time.sleep(0.1)
            for json_response in temp_ls:
                matching_rule=json_response.get("matching_rules")[0].get('tag')
                if matching_rule=='covid':
                    id_covid+=1
                    json_response.update({'ID':id_covid})
                    json_response.update({'MR':matching_rule})
                    ls_covid.append(json.dumps(json_response,sort_keys=True))
                if id_covid > int(sys.argv[1]):
                    stop_stream=1
            print('sleeping')
            time.sleep(5)
            #thread_stream.join()
        elif id_covid > int(sys.argv[1]) and id_covid_vaccine <= int(sys.argv[1]):
            stop_stream=0
            temp_ls.clear()
            connection_established=0
            print("two more")
            thread_stream2=threading.Thread(target=twit_stream,args=(req_rules2,))
            thread_stream2.start()
            while(connection_established==0):
                time.sleep(0.1)
            for json_response in temp_ls:
                if json_response==None and stop_stream==0:
                    pass
                else:
                    print(json_response)
                    matching_rule=json_response.get("matching_rules")[0].get('tag')
                    if matching_rule=='vaccine':
                        id_covid_vaccine+=1
                        json_response.update({"ID":id_covid_vaccine})
                        json_response.update({'MR':matching_rule})
                        ls_vaccine.append(json.dumps(json_response, sort_keys=True))
                    if id_covid_vaccine > int(sys.argv[1]):
                        stop_stream=1
            print('sleeping')
            time.sleep(5)
            #thread_stream.join()
        else:
            break
    print('ls_covid')
    print(ls_covid)
    print('ls_vaccine')
    print(ls_vaccine)
