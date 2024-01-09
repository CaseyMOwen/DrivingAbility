import RedditAPI
import config
import pandas as pd
import csv
import os.path

class Querier:
    posts_data_file = 'post_data.csv'

    def __init__(self):
        self.reddit = RedditAPI.RedditAPI(config.reddit_username, config.reddit_password, config.client_id, config.client_secret, config.user_agent)
        states_dict = self.create_states_dict()
        self.posts_df = self.read_posts_data()

    # Subreddit dict is dict where key is friendly name of subreddit, and value is the subreddit name. Appends to existing dataframe, which by default is false. Dataframe must give id field and created_utc field for pagination 
    def get_posts_data(self, subreddit_dict, calls_per_subreddit=1, existing_df = pd.DataFrame()):
        
        dfs = [None] * calls_per_subreddit * len(subreddit_dict)
        for i, subreddit_key in enumerate(subreddit_dict):
            subreddit = subreddit_dict[subreddit_key]
            subreddit_filtered_df = existing_df.loc[existing_df['subreddit'] == subreddit]
            oldest_recorded = "t3_" + subreddit_filtered_df['id'].iloc[subreddit_filtered_df['created_utc'].argmin()]
            newest_recorded = "t3_" + subreddit_filtered_df['id'].iloc[subreddit_filtered_df['created_utc'].argmax()]
            # i.e. state 2 will go in index 1, 51, 101 ...
            df_idx = len(subreddit_dict)*i
            received_df, after = self.reddit.get_subreddit_posts(subreddit)

            dfs[i] = self.reddit.project_and_clean_key_columns(received_df)
        df = pd.concat(dfs)
        df.to_csv(self.posts_data_file)

    def read_posts_data(self):
        if os.path.isfile(self.posts_data_file):
            df = pd.read_csv(self.posts_data_file)
        else:
            df = pd.DataFrame()
        return df
    
    def write_posts_data(self):
        self.posts_df.to_csv(self.posts_data_file)

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
        return filtered_df['id'].iloc[filtered_df['created_utc'].argmax()]



q = Querier()
# q.collect_posts_data()
small_states_dict = {'texas': 'Texas', 'california': 'California'}
print(q.get_newest_recorded_post('texas'))