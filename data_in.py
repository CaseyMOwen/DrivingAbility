import requests
import json
import config


print(config.reddit_password)

import requests.auth
client_auth = requests.auth.HTTPBasicAuth('p-jcoLKBynTLew', 'gko_LXELoV07ZBNUXrvWZfzE3aI')
post_data = {"grant_type": "password", "username": "reddit_bot", "password": "snoo"}
headers = {"User-Agent": "ChangeMeClient/0.1 by YourUsername"}
response = requests.post("https://www.reddit.com/api/v1/access_token", auth=client_auth, data=post_data, headers=headers)
# response.json()
# json_object = json.dumps(response.json())
with open("access_token.json", "w") as outfile:
    outfile.write(json.dumps(response.json()))


def request_authorization():
    url = 'https://www.reddit.com/api/v1/authorize'
    parameters = {
        'client_id': '',
        'response_type': 'code',
        'state': '9786211864223186',
        'redirect_uri': 'http://localhost:8080',
        'duration': 'permanant',
        'scope': 'read'
    }
    r = requests.get(url, params=parameters)
    print(r)
    r_json = r.json()
    json_object = json.dumps(r_json)
    print(json_object)

def retrieve_token():
    url = 'https://www.reddit.com/api/v1/access_token'
    data = {
        'grant_type': '',
        'username': '',
        'password': ''
    }
    x = requests.post(url, data=data)
    x_json = x.json()
    json_object = json.dumps(x_json)
    with open("access_token.json", "w") as outfile:
        outfile.write(json_object)
    # return x

retrieve_token()
# base_url = 'https://www.reddit.com'
# subreddit = '/r/Connecticut'
# r = requests.get(base_url+subreddit+'/new.json')
# r_json = r.json()
# json_object = json.dumps(r_json)
# with open("testdata.json", "w") as outfile:
#     outfile.write(json_object)
