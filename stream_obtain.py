import json
import sys
import requests
import os

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
    # global stop_stream
    print('1')
    bearer_token=os.environ.get("BEARER_TOKEN")
    bearer_str="Bearer "+str(bearer_token)
    header={"Authorization":bearer_str}
    refine_rules(header,req_rules)
    
    params={'tweet.fields':'public_metrics'}
    #params={'tweet.fields':'created_at','tweet.fields':'referenced_tweets','tweet':'retweet_count'}
    response=requests.get('https://api.twitter.com/2/tweets/search/stream',params=params,headers=header,stream=True)
    i=0
    connection_established=1
    for line in response.iter_lines():
        global stop_stream
        if global stop_stream==1:
            return
        else:
            json_response = json.loads(line)  #json.loads----->Deserialize fp (a .read()-supporting text file or binary file containing a JSON document) to a Python object using this conversion table.ie json to python object 
            i+=1
            temp_ls.append(json_response)
    return


