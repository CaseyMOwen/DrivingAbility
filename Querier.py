import RedditAPI
import config
import pandas as pd
import csv
import os.path

class Querier:
    posts_data_file = 'post_data.csv'

    def __init__(self):
        self.reddit = RedditAPI.RedditAPI(config.reddit_username, config.reddit_password, config.client_id, config.client_secret, config.user_agent)
        self.states_dict = self.create_states_dict()
        self.posts_df = self.read_posts_data()

    # Subreddit dict is dict where key is friendly name of subreddit, and value is the subreddit name. Appends to existing dataframe, which by default is false. Dataframe must give id field and created_utc field for pagination 
    def get_older_posts(self, subreddit_dict, max_calls_per_subreddit=1, existing_df = pd.DataFrame()):
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
                    after = "t3_" + subreddit_filtered_df['id'].iloc[subreddit_filtered_df['created_utc'].argmin()]
            call_count = 0
            hit_max_history = False
            while call_count < max_calls_per_subreddit and not hit_max_history:
            # for j in range(max_calls_per_subreddit):
                
                # oldest_recorded = "t3_" + subreddit_filtered_df['id'].iloc[subreddit_filtered_df['created_utc'].argmin()]
                # newest_recorded = "t3_" + subreddit_filtered_df['id'].iloc[subreddit_filtered_df['created_utc'].argmax()]
                # i.e. state 1 will go in 1,2,3, state 2 goes in 4,5,6
                df_idx = (max_calls_per_subreddit*i) + call_count
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

small_states_dict = {'Texas': 'texas', 'California': 'california'}
# q.get_older_posts(small_states_dict, max_calls_per_subreddit=2)
# df = q.read_posts_data()
# q.get_older_posts(small_states_dict, max_calls_per_subreddit=2, existing_df=df)

one_state_dict ={'Texas': 'texas'}
q.get_older_posts(one_state_dict, max_calls_per_subreddit=12)
df = q.read_posts_data()
print(f'Number of records collected:{df.shape[0]}')
print(df['id'].is_unique)
