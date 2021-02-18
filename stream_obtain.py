import json
import sys
import requests
import os

def refine_rules(header):
    current_rules=requests.get("https://api.twitter.com/2/tweets/search/stream/rules",headers=header)
    print(json.dumps(current_rules.json()))
    rules=[{'value':'covid','tag':'covid'},{'value':'covid vaccine','tag':'covid vaccine'}]
    to_add={'add':rules}
    new_rules=requests.post("https://api.twitter.com/2/tweets/search/stream/rules",headers=header,json=to_add)
    current_rules=requests.get("https://api.twitter.com/2/tweets/search/stream/rules",headers=header)
    print(json.dumps(current_rules.json()))
def main(N):
    bearer_token=os.environ.get("BEARER_TOKEN")
    bearer_str="Bearer "+str(bearer_token)
    header={"Authorization":bearer_str}
    refine_rules(header)
    response=requests.get('https://api.twitter.com/2/tweets/search/stream',headers=header,stream=True)
    #print(dir(response.iter_lines))
    i=0
    for line in response.iter_lines():
        if i<3:
            json_response = json.loads(line)
            print(json.dumps(json_response, indent=4, sort_keys=True))
            i+=1
        else:
            break

if __name__=="__main__":
    main(sys.argv[1])

