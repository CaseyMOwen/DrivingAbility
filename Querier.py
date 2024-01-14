import RedditAPI
import config
import pandas as pd
import csv
import os.path
import praw
from anyascii import anyascii

class Querier:
    posts_data_file = 'post_data.parquet'

    def __init__(self):
        self.reddit = RedditAPI.RedditAPI(config.reddit_username, config.reddit_password, config.client_id, config.client_secret, config.user_agent)
        self.states_dict = self.create_states_dict()
        self.posts_df = self.read_posts_data()

    # Subreddit dict is dict where key is friendly name of subreddit, and value is the subreddit name. Appends to existing dataframe, which by default is false. Dataframe must give id field and created_utc field for pagination 
    def get_older_posts(self, subreddit_dict,  existing_df = pd.DataFrame(),max_calls_per_subreddit=1):
        num_subreddits = len(subreddit_dict)
        dfs = [None] * max_calls_per_subreddit * num_subreddits
        for i, subreddit_key in enumerate(subreddit_dict):
            subreddit = subreddit_dict[subreddit_key]
            # After = None means we have no records and need to start from the beginning
            after = None
            if not existing_df.empty:
                subreddit_filtered_df = existing_df.loc[existing_df['subreddit'] == subreddit]
                # Still need to check that that subreddit specifically has posts
                if not subreddit_filtered_df.empty:
                    # Use oldest recorded
                    after = "t3_" + subreddit_filtered_df['id'].loc[subreddit_filtered_df['created_utc'].idxmin()]
            call_count = 0
            hit_max_history = False
            while call_count < max_calls_per_subreddit and not hit_max_history:
            # for j in range(max_calls_per_subreddit):
                
                # oldest_recorded = "t3_" + subreddit_filtered_df['id'].iloc[subreddit_filtered_df['created_utc'].argmin()]
                # newest_recorded = "t3_" + subreddit_filtered_df['id'].iloc[subreddit_filtered_df['created_utc'].argmax()]
                # i.e. state 1 will go in 1,2,3, state 2 goes in 4,5,6
                df_idx = (max_calls_per_subreddit*i) + call_count
                print(f'Getting older posts for state {i}, {subreddit}, call {call_count}')
                received_df, after, before = self.reddit.get_subreddit_posts(subreddit, after=after)
                if after == None:
                    # Reddit has a max available post history of about 1000 per subreddit, after tag is returned as None when this limit is hit
                    hit_max_history = True
                dfs[df_idx] = self.reddit.project_and_clean_key_columns(received_df)
                call_count += 1
                # if call_count == 9:
                #     print("10 calls")
        dfs.insert(0,existing_df)
        df = pd.concat(dfs)
        df.to_parquet(self.posts_data_file)

    # In this case having an existing dataframe is mandatory, and posts newer than what dataframe has are retrieved. If there are no posts of the given subreddit in that dataframe, then that number of older posts is retreived instead
    def get_newer_posts(self, subreddit_dict, existing_df, max_calls_per_subreddit=1):
        num_subreddits = len(subreddit_dict)
        dfs = [None] * max_calls_per_subreddit * num_subreddits
        for i, subreddit_key in enumerate(subreddit_dict):
            subreddit = subreddit_dict[subreddit_key]
            if existing_df.empty:
                raise Exception("You must pass in a non-empty data frame as a reference to get newer posts from")
            else: 
                subreddit_filtered_df = existing_df.loc[existing_df['subreddit'] == subreddit]
                # Still need to check that that subreddit specifically has posts
                if not subreddit_filtered_df.empty:
                    # Use newest recorded as a starting point
                    before = "t3_" + subreddit_filtered_df['id'].loc[subreddit_filtered_df['created_utc'].idxmax()]
                else:
                    # The dataframe is not empty, but that subreddit has not posts
                    print(f'Warning: cannot get newer posts for subreddit {subreddit}, since it has not post history recorded')
                    continue
            call_count = 0
            hit_max_history = False
            while call_count < max_calls_per_subreddit and not hit_max_history:
                # i.e. state 1 will go in 1,2,3, state 2 goes in 4,5,6
                df_idx = (max_calls_per_subreddit*i) + call_count
                limit = 100
                print(f"getting before: {before} at subreddit {subreddit}")
                received_df, after, before = self.reddit.get_subreddit_posts(subreddit, before=before, limit=limit)
                # First Value of recieved dataframe is before:
                before = "t3_" + before
                if received_df.shape[0] < limit:
                    # Reddit has a max available post history of about 1000 per subreddit, after tag is returned as None when this limit is hit
                    hit_max_history = True
                dfs[df_idx] = self.reddit.project_and_clean_key_columns(received_df)
                call_count += 1
                # if call_count == 9:
                #     print("10 calls")
        dfs.insert(0,existing_df)
        df = pd.concat(dfs)
        df.sort_values(by=['subreddit', 'created_utc'])
        df.to_parquet(self.posts_data_file)

    def read_posts_data(self):
        if os.path.isfile(self.posts_data_file):
            df = pd.read_parquet(self.posts_data_file)
            df['created_utc'] = pd.to_datetime(df['created_utc'])
        else:
            df = pd.DataFrame()
        return df
    
    def write_posts_data(self):
        self.posts_df.to_parquet(self.posts_data_file)

    # Where key is the state, and value is the subreddit
    def create_states_dict(self):
        with open('StateSubreddits.csv') as f:
            next(f)  # Skip the header
            reader = csv.reader(f, skipinitialspace=True)
            states_dict = dict(reader)
        return states_dict
    
    def get_oldest_recorded_post(self, df, subreddit):
        filtered_df = df.loc[df['subreddit'] == subreddit]
        return filtered_df['id'].iloc[filtered_df['created_utc'].argmin()]
    
    def get_newest_recorded_post(self, df, subreddit):
        filtered_df = df.loc[df['subreddit'] == subreddit]
        return filtered_df['id'].loc[filtered_df['created_utc'].idxmax()]

# q = Querier()
# # df = q.praw_get_posts_and_comments("texas",limit=5)
# df = q.praw_get_comments('196elwq')
# print(df)

# q = Querier()

# small_states_dict = {'Texas': 'texas', 'California': 'california'}
# # q.get_older_posts(small_states_dict, max_calls_per_subreddit=2)
# # df = q.read_posts_data()
# # q.get_older_posts(small_states_dict, max_calls_per_subreddit=2, existing_df=df)

# # one_state_dict ={'Texas': 'texas'}

# states_dict = q.create_states_dict()

# q.get_older_posts(states_dict, max_calls_per_subreddit=12)
# df = pd.read_parquet(q.posts_data_file)
# print(df['id'].is_unique)
# print(df[df[['id']].duplicated() == True])


# df = pd.read_parquet("test_post_data.parquet")
# df['created_utc'] = pd.to_datetime(df['created_utc'])
# q.get_newer_posts(small_states_dict, existing_df=df, max_calls_per_subreddit=7)
# new_df = q.read_posts_data()
# print(f'Original number of records: {df.shape[0]}')
# print(f'New number of records collected:{new_df.shape[0]}')
# print(df['id'].is_unique)
# print(new_df['id'].is_unique)
# print(new_df[new_df[['id']].duplicated() == True])

# df = q.reddit.get_limited_post_comments(subreddit='Connecticut', id='1938uqq')
# print(df)