# Update Posts locally, every so often
import pandas as pd
import RedditCollector
import Analyzer
import config
import boto3
from datetime import datetime, timezone
import json

posts_filename = 'post_data.parquet'
comments_filename = 'comments_data.parquet'
scored_posts_filename = 'scored_post_data.parquet'
scored_comments_filename = 'scored_comment_data.parquet'
stats_filename = 'stats.json'


def main():
    get_posts = 0
    get_comments = 0
    score_posts = 1000
    score_comments = 0
    update_local_files(get_posts,get_comments,score_posts,score_comments)
    # update_s3_if_needed()
    # print(get_last_modified(config.AWS_BUCKET_NAME))
    update_file_stats()
    # df = pd.read_parquet(scored_comments_filename)
    # df.to_csv('test_scored_comment_data.csv')

def update_s3_if_needed():
    today = datetime.now(timezone.utc).date()
    last_modified = get_last_modified(config.AWS_BUCKET_NAME)
    if last_modified.date() < today:
        print("need to update")
        # write_all_s3()
        print("updated")
    else:
        print("no need to update")

def update_local_files(collect_posts, collect_comment_posts, analyze_posts, analyze_comments):
    posts_df, comments_df = scrape(collect_posts, collect_comment_posts)
    analyze(posts_df, comments_df, analyze_posts, analyze_comments)

# def analyze_post(post_id):
#     posts_df, comments_df = scrape(0, 0)
#     a = Analyzer.Analyzer(posts_df, comments_df, scored_posts_filename, scored_comments_filename)
def update_file_stats():
    stats = {}
    posts_df = pd.read_parquet(posts_filename)
    stats["num_posts"] = posts_df.shape[0]
    comments_df = pd.read_parquet(comments_filename)
    stats["num_comments"] = comments_df.shape[0]
    scored_posts_df = pd.read_parquet(scored_posts_filename)
    stats["num_scored_posts"] = scored_posts_df.shape[0]
    scored_comments_df = pd.read_parquet(scored_comments_filename)
    stats["num_scored_comments"] = scored_comments_df.shape[0]
    with open(stats_filename, "w") as outfile:
        json.dump(stats, outfile)


def scrape(posts_per_subreddit, num_posts_for_comments):
    state_subreddits_df = pd.read_csv('StateSubreddits.csv')
    state_subreddits_list = state_subreddits_df['Subreddit'].to_list()
    rc = RedditCollector.RedditCollector(state_subreddits_list,posts_filename, comments_filename)
    rc.update_local_post_data(limit=posts_per_subreddit)
    rc.update_local_comments_data(max_num_posts=num_posts_for_comments)
    posts_df = rc.read_local_posts_data()
    comments_df = rc.read_local_comments_data()
    return posts_df, comments_df

def analyze(posts_df, comments_df, score_num_posts, score_num_comments):
    a = Analyzer.Analyzer(posts_df, comments_df, scored_posts_filename, scored_comments_filename)
    a.update_score_posts(score_num_posts)
    a.update_score_comments(score_num_comments)

# Writes an existing local file to s3
def write_s3(local_filename, bucketname, s3_filename):
    s3 = boto3.resource('s3', region_name=config.AWS_DEFAULT_REGION,aws_access_key_id=config.AWS_ACCESS_KEY_ID, aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY)
    s3.Bucket(bucketname).upload_file(local_filename,s3_filename)

def get_last_modified(bucketname):
    s3 = boto3.resource('s3', region_name=config.AWS_DEFAULT_REGION,aws_access_key_id=config.AWS_ACCESS_KEY_ID, aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY)
    bucket = s3.Bucket(bucketname)
    last_modifieds = []
    for object in bucket.objects.all():
        last_modifieds.append(object.last_modified)
    youngest = max(last_modifieds)
    return youngest

def write_all_s3():
    print("Writing posts file")
    write_s3(posts_filename, config.AWS_BUCKET_NAME, posts_filename)
    print("Writing comments file")
    write_s3(comments_filename, config.AWS_BUCKET_NAME, comments_filename)
    print("Writing post scores file")
    write_s3(scored_posts_filename, config.AWS_BUCKET_NAME, scored_posts_filename)
    print("Writing comment scores file")
    write_s3(scored_comments_filename, config.AWS_BUCKET_NAME, scored_comments_filename)

if __name__ == '__main__':
    main()