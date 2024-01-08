import requests
import json
import config
import requests.auth
import pandas as pd
from anyascii import anyascii
import csv

# Request a new token, that lasts for one day. Record it to the access token json file
def request_token():
    client_auth = requests.auth.HTTPBasicAuth(config.client_id, config.client_secret)
    post_data = {"grant_type": "password", "username": config.reddit_username, "password": config.reddit_password}
    headers = {"User-Agent": config.user_agent}
    response = requests.post("https://www.reddit.com/api/v1/access_token", auth=client_auth, data=post_data, headers=headers)
    # dict = response.json()
    # json_object = json.dumps(response.json())
    with open("access_token.json", "w") as outfile:
        outfile.write(json.dumps(response.json()))

# Trys to request data, with error handling. If token is expired, request a new one once, otherwise error
def try_request(url, headers, params, first_try=True):
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
    except requests.exceptions.ConnectionError as err:
        # eg, no internet
        raise SystemExit(err)
    except requests.exceptions.HTTPError as err:
        # eg, url, server and other errors
        if response.status_code == 401 and first_try == True:
            print("Unauthorized, requesting new token and trying again")
            request_token()
            try_request(url, headers, params, first_try=False)
        else:
            raise SystemExit(err)
    except requests.exceptions.Timeout as err:
        raise SystemExit(err)
    except requests.exceptions.TooManyRedirects as err:
        raise SystemExit(err)
    return response

# retrieve_token()
def get_subreddit_posts(subreddit):
    # TODO: OOP, make token a class variable so don't have to open this file everytime
    token_file = open('access_token.json')
    token_data = json.load(token_file)
    token = token_data['access_token']
    headers = {"Authorization": "bearer " + token, "User-Agent": config.user_agent}
    params = {'raw_json': 1, 'limit': 100}
    base_url = 'https://oauth.reddit.com'
    # subreddit = 'Connecticut'
    r = try_request(base_url+'/r/'+subreddit+'/new.json',headers=headers, params=params)
    r_json = r.json()
    # json_object = json.dumps(r_json)
    # with open("testdata.json", "w") as outfile:
    #     outfile.write(json_object)
    df = posts_json_to_df(r_json)
    return df

def posts_json_to_df(posts_json):
    # In future - just return json object in function. This is temporary to avoid too many requests
    # post_file = open('testdata.json')
    # post_data = json.load(post_file)
    # posts = post_data["data"]["children"]
    posts = posts_json["data"]["children"]
    df = pd.json_normalize(posts)
    
    # print(list(df.columns))
    columns_to_project = ['data.subreddit', 'data.created_utc', 'data.title', 'data.id', 'data.selftext']
    projected_df = df[columns_to_project]
    projected_df.columns = [col.replace('data.', '') for col in projected_df.columns]
    projected_df['selftext'] = projected_df['selftext'].map(lambda text: anyascii(text))
    projected_df['title'] = projected_df['title'].map(lambda text: anyascii(text))
    # print(projected_df)
    # projected_df.to_csv('post_data.csv')
    return projected_df

def collect_all_data():
    states_dict = create_states_dict()
    # print(states_dict)
    small_states_dict = {'texas': 'Texas', 'california': 'California'}
    calls_per_state = 1
    dfs = [None] * calls_per_state * len(small_states_dict)
    for i, state in enumerate(small_states_dict):
        subreddit = small_states_dict[state]
        dfs[i] = get_subreddit_posts(subreddit)
    df = pd.concat(dfs)
    df.to_csv('post_data.csv')
    # connecticut_df = get_subreddit_posts('Connecticut')
    # print(connecticut_df)

# Where key is the state, and value is the subreddit
def create_states_dict():
    with open('StateSubreddits.csv') as f:
        next(f)  # Skip the header
        reader = csv.reader(f, skipinitialspace=True)
        states_dict = dict(reader)
    return states_dict

# record_post_data()
collect_all_data()