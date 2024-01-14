import RedditAPI
import config
import pandas as pd
import csv
import os.path
import praw
from anyascii import anyascii
import boto3

class RedditCollector:
    

    def __init__(self, subreddit_list, local_posts_data_file, local_comments_data_file):
        self.reddit = praw.Reddit(
            client_id=config.client_id,
            client_secret=config.client_secret,
            user_agent=config.user_agent,
        )
        self.subreddit_list = subreddit_list
        self.local_posts_data_file = local_posts_data_file
        self.local_comments_data_file = local_comments_data_file
        # self.states_dict = self.create_states_dict()
        # self.posts_df = self.read_local_posts_data()


    def update_local_post_data(self, limit=None):      
        existing_df = self.read_local_posts_data()
        subreddit_dfs = []
        for i, subreddit in enumerate(self.subreddit_list):
            print(f"Getting from subreddit {subreddit}, number {i}")
            subreddit_df = self.get_subreddit_posts(subreddit=subreddit, limit=limit)
            subreddit_dfs.append(subreddit_df)
        full_df = pd.concat(subreddit_dfs)
        merged_df = pd.concat([full_df, existing_df])
        merged_df = merged_df[~merged_df.index.duplicated(keep='first')]
        merged_df.to_parquet(self.local_posts_data_file)

    def update_local_comments_data(self, max_num_posts):
        existing_comments_df = self.read_local_comments_data()
        existing_posts_df = self.read_local_posts_data()
        comment_dfs = []
        # print(existing_posts_df)
        post_ids = list(existing_posts_df.index.values)
        post_count = 0
        existing_comments_list = existing_comments_df['post_id'].to_list()
        for post_id in post_ids:
            if post_count == max_num_posts:
                break
            if post_id in existing_comments_list:
                continue
            else:
                comment_df = self.get_comments(post_id)
                post_count += 1
                comment_dfs.append(comment_df)
        full_df = pd.concat(comment_dfs)
        merged_df = pd.concat([full_df, existing_comments_df])
        merged_df = merged_df[~merged_df.index.duplicated(keep='first')]
        # print(merged_df)
        merged_df.to_parquet(self.local_comments_data_file)



    def read_local_posts_data(self):
        if os.path.isfile(self.local_posts_data_file):
            df = pd.read_parquet(self.local_posts_data_file)
            df.index.name = 'id'
        else:
            df = pd.DataFrame(columns=['title', 'selftext', 'created_utc', 'subreddit'])
            df.index.name = 'id'
        return df
    
    def read_local_comments_data(self):
        if os.path.isfile(self.local_comments_data_file):
            df = pd.read_parquet(self.local_comments_data_file)
            df.index.name = 'id'
        else:
            df = pd.DataFrame(columns=['body', 'post_id'])
            df.index.name = 'id'
        return df
    
    def get_subreddit_posts(self, subreddit, limit=None):
        submission_df_entries = []
        for submission in self.reddit.subreddit(subreddit).new(limit=limit):
            submission_df_entry = pd.DataFrame.from_dict({
                "id":[submission.id],
                "title":[anyascii(submission.title)],
                "selftext":[anyascii(submission.selftext)],
                "created_utc":[submission.created_utc],
                "subreddit":[subreddit]
            }).set_index('id')
            submission_df_entries.append(submission_df_entry)
        submission_df = pd.concat(submission_df_entries)
        return submission_df
    
    # Writes an existing local file to s3
    def write_s3(self, local_filename, bucketname, s3_filename):
        s3 = boto3.resource('s3', region_name=config.AWS_DEFAULT_REGION,aws_access_key_id=config.AWS_ACCESS_KEY_ID, aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY)
        s3.Bucket(bucketname).upload_file(local_filename,s3_filename)

    def get_comments(self, post_id):
        print(f'getting comments for post {post_id}')
        submission = self.reddit.submission(post_id)
        submission.comments.replace_more(limit=None)
        comment_df_entries = []
        for comment in submission.comments.list():
            comment_df_entry = pd.DataFrame.from_dict({
                "id":[comment.id],
                "body":[anyascii(comment.body)],
                "post_id":[comment.submission.id]
            }).set_index('id')
            comment_df_entries.append(comment_df_entry)
        if not comment_df_entries:
            comment_df = None
        else:
            comment_df = pd.concat(comment_df_entries)
        return comment_df


state_subreddits_df = pd.read_csv('StateSubreddits.csv')
state_subreddits_list = state_subreddits_df['Subreddit'].to_list()
rc = RedditCollector(state_subreddits_list,'post_data.parquet', 'comments_data.parquet')
rc.update_local_post_data()
# rc.update_local_comments_data(10)
# rc.write_s3('post_data.parquet', config.AWS_BUCKET_NAME, 'post_data.parquet')
# print(rc.read_local_comments_data())
print(rc.read_local_posts_data())