import requests
import json
import config
import requests.auth
import pandas as pd
from anyascii import anyascii
import os.path
import datetime

class RedditAPI:
    token_filename = "Reddit_Access_Token.json"

    def __init__(self, reddit_username, reddit_password, client_id, client_secret, user_agent):
        self.reddit_username = reddit_username
        self.reddit_password = reddit_password
        self.client_id = client_id
        self.client_secret = client_secret
        self.user_agent = user_agent
        self.token = self.read_token()

    # Request a new token, that lasts for one day. Record it to the access token json file
    def request_token(self):
        client_auth = requests.auth.HTTPBasicAuth(self.client_id, self.client_secret)
        post_data = {"grant_type": "password", "username": self.reddit_username, "password": self.reddit_password}
        headers = {"User-Agent": self.user_agent}
        response = requests.post("https://www.reddit.com/api/v1/access_token", auth=client_auth, data=post_data, headers=headers)
        # dict = response.json()
        # json_object = json.dumps(response.json())
        with open(self.token_filename, "w") as outfile:
            outfile.write(json.dumps(response.json()))

    # Reads existing token from the file
    def read_token(self):
        if not os.path.isfile(self.token_filename):
            self.request_token()
        token_file = open(self.token_filename)
        token_data = json.load(token_file)
        token = token_data['access_token']
        return token
    
    # Trys to request data, with error handling. If token is expired, request a new one once, otherwise error
    def try_request(self, url, headers, params, first_try=True):
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
        except requests.exceptions.ConnectionError as err:
            # eg, no internet
            raise SystemExit(err)
        except requests.exceptions.HTTPError as err:
            # eg, url, server and other errors
            if response.status_code == 401 and first_try == True:
                # 401 error likely means token has expired
                print("Unauthorized, requesting new token and trying again")
                self.request_token()
                self.try_request(url, headers, params, first_try=False)
            else:
                raise SystemExit(err)
        except requests.exceptions.Timeout as err:
            raise SystemExit(err)
        except requests.exceptions.TooManyRedirects as err:
            raise SystemExit(err)
        return response

    # retrieve_token()
    def get_subreddit_posts(self, subreddit, after=None, before=None, sort_by='new', limit=100):
        headers = {"Authorization": "bearer " + self.token, "User-Agent": config.user_agent}
        params = {'raw_json': 1, 'limit': limit}
        if before is not None and after is not None:
            raise Exception("Only one of before and after can be set at a time")
        elif after is not None:
            params['after'] = after
        elif before is not None:
            params['before'] = before
        base_url = 'https://oauth.reddit.com'
        r = self.try_request(base_url+'/r/'+subreddit+'/'+ sort_by + '.json',headers=headers, params=params)
        r_json = r.json()
        next_after = r_json["data"]["after"]
        next_before = r_json["data"]["children"][0]['data']['id']
        # if next_after == 'null':
        #     next_after = None
        # if next_before == 'null':
        #     next_before = None
        df = self.posts_json_to_df(r_json, subreddit)
        return df, next_after, next_before

    
    

    def get_post_comments(self, subreddit, id, sort_by='top', limit=100, depth=10):
        headers = {"Authorization": "bearer " + self.token, "User-Agent": config.user_agent}
        params = {'raw_json': 1, 'limit': limit, 'depth': depth}
        base_url = 'https://oauth.reddit.com'
        r = self.try_request(base_url+'/r/'+subreddit+'/comments/'+ id + '/' + sort_by + '.json', headers=headers, params=params)
        r_json = r.json()
        root_comments = r_json[1]['data']['children']
        comment_dfs = []
        for root in root_comments:
            comment_df = self.get_comment_and_replies(root, subreddit)
            comment_dfs.append(comment_df)
        all_comments_df = pd.concat(comment_dfs)
        all_comments_df['parent_id'] = id
        all_comments_df['subreddit'] = subreddit
        return all_comments_df


    def get_comment_and_replies(self, comment_tree, subreddit):      
        if 'body' in comment_tree['data'] and 'created_utc' in comment_tree['data']:
            comment_text = comment_tree['data']['body']
            utc = comment_tree['data']['created_utc']
            d = {'id': comment_tree['data']['id'], 'text': anyascii(comment_text), 'created_utc': datetime.datetime.fromtimestamp(utc)}
            comment_df = pd.DataFrame(d, index=[0])
        else:
            # Case where fields necessary to the comment and text are missing
            comment_df = None
        if 'replies' not in comment_tree['data'] or comment_tree['data']['replies'] == "":
            # Case where there are no replies (bottom level comment), or replies field is missing
            return comment_df
        else:
            replies = comment_tree['data']['replies']
            reply_dfs = []
            for reply in replies['data']['children']:
                reply_df = self.get_comment_and_replies(reply, subreddit)
                reply_dfs.append(reply_df)
            reply_dfs.append(comment_df)
            return pd.concat(reply_dfs)

    def posts_json_to_df(self, posts_json, subreddit):
        posts = posts_json["data"]["children"]
        df = pd.json_normalize(posts)
        df.columns = [col.replace('data.', '') for col in df.columns]
        df = df.drop('subreddit', axis=1)
        df['subreddit'] = subreddit
        return df
    
    def clean_text_columns(self, df, columnnames):
        # turning off warning temporarily since it comes up mistakenly for lambda function
        pd.options.mode.chained_assignment = None
        for columnname in columnnames:
            df[columnname] = df.loc[:, [columnname]].map(lambda text: anyascii(text))
            # df['cleancolumn'] = df.apply(lambda row: anyascii(row[columnname]), axis=1)
            # df.drop(columns=[columnname])
            # df = df.rename(columns={'cleancolumn':columnname})
        pd.options.mode.chained_assignment = 'warn'
        return df
    
    def convert_timestamps(self, df, time_col_name = 'created_utc'):
        pd.options.mode.chained_assignment = None
        df[time_col_name] = df.loc[:, [time_col_name]].map(lambda timestamp: datetime.datetime.fromtimestamp(timestamp))
        # df[time_col_name] = pd.to_datetime(df[time_col_name], unit='s')
        pd.options.mode.chained_assignment = 'warn'
        return df
    
    def project_and_clean_key_columns(self, df, columns_to_project = ['subreddit', 'created_utc', 'title', 'id', 'selftext'], columns_to_clean = ['selftext', 'title']):
        # columns_to_project = ['subreddit', 'created_utc', 'title', 'id', 'selftext']
        projected_df = df[columns_to_project]
        clean_df = self.clean_text_columns(projected_df, columns_to_clean)
        clean_df2 = self.convert_timestamps(clean_df)
        return clean_df2



    
