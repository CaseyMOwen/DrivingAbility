from transformers import pipeline
import pandas as pd
import RedditCollector
import os.path
import torch
from transformers import AutoModelForSequenceClassification, AutoTokenizer

class Analyzer():
    def __init__(self, posts_df, comments_df, local_scored_posts_file, local_scored_comments_file):
        self.posts_df = posts_df
        self.comments_df = comments_df
        # self.classifier = pipeline("zero-shot-classification",
                    #   model="facebook/bart-large-mnli")
        self.local_scored_posts_file = local_scored_posts_file
        self.local_scored_comments_file = local_scored_comments_file
        self.candidate_labels = ['drivers', 'complaining', 'my state has bad drivers']
        self.tokenizer = AutoTokenizer.from_pretrained('facebook/bart-large-mnli')
        self.device = 'cuda'
        with torch.device(self.device):
            self.nli_model = AutoModelForSequenceClassification.from_pretrained('facebook/bart-large-mnli', torch_dtype=torch.float16)
        
        
    def score_text(self, label, text):
        label = 'driving'
        # premise = 'Drivers here are the worst'
        hypothesis = f'This example is {label}.'

        # run through model pre-trained on MNLI
        x = self.tokenizer.encode(text, hypothesis, return_tensors='pt',
                            truncation='only_first')
        logits = self.nli_model(x.to(self.device))[0]

        # we throw away "neutral" (dim 1) and take the probability of
        # "entailment" (2) as the probability of the label being true 
        entail_contradiction_logits = logits[:,[0,2]]
        probs = entail_contradiction_logits.softmax(dim=1)
        prob_label_is_true = probs[:,1].item()
        return prob_label_is_true

    def update_score_posts(self, max_num_posts):
        if max_num_posts == 0:
            return
        existing_scored_df = self.read_local_scored_posts_data()
        subreddit_counts =  self.posts_df.groupby('subreddit')['subreddit'].transform('count')
        # weights = 1/subreddit_counts
        randomized_posts_df = self.posts_df.sample(frac=1, weights=1/subreddit_counts)
        post_classified_df_entries = []
        count = 0
        existing_posts_list = existing_scored_df.index.to_list()
        for post_id in randomized_posts_df.index:
            if count == max_num_posts:
                break
            if post_id in existing_posts_list:
                continue
            print(f'scoring post {post_id}, number {count}')
            subreddit = self.posts_df.loc[post_id, 'subreddit']
            post_classified_df_entry = self.score_post(post_id, subreddit)
            post_classified_df_entries.append(post_classified_df_entry)
            count += 1
        full_df = pd.concat(post_classified_df_entries)
        merged_df = pd.concat([full_df, existing_scored_df])
        merged_df = merged_df[~merged_df.index.duplicated(keep='first')]
        merged_df.to_parquet(self.local_scored_posts_file)

    def update_score_comments(self, max_num_comments):
        if max_num_comments == 0:
            return
        existing_scored_df = self.read_local_scored_comments_data()
        subreddit_counts =  self.comments_df.groupby('subreddit')['subreddit'].transform('count')
        # weights = 1/subreddit_counts
        randomized_comments_df = self.comments_df.sample(frac=1, weights=1/subreddit_counts)
        comment_classified_df_entries = []
        count = 0
        existing_comments_list = existing_scored_df.index.to_list()
        for comment_id in randomized_comments_df.index:
            if count == max_num_comments:
                break
            if comment_id in existing_comments_list:
                continue
            print(f'scoring comment {comment_id}, number {count}')
            subreddit = self.comments_df.loc[comment_id, 'subreddit']
            comment_classified_df_entry = self.score_comment(comment_id, subreddit)
            comment_classified_df_entries.append(comment_classified_df_entry)
            count += 1
        full_df = pd.concat(comment_classified_df_entries)
        merged_df = pd.concat([full_df, existing_scored_df])
        merged_df = merged_df[~merged_df.index.duplicated(keep='first')]
        merged_df.to_parquet(self.local_scored_comments_file)

    def read_local_scored_posts_data(self):
        if os.path.isfile(self.local_scored_posts_file):
            df = pd.read_parquet(self.local_scored_posts_file)
            df.index.name = 'id'
        else:
            df = pd.DataFrame()
            df.index.name = 'id'
        return df
    
    def read_local_scored_comments_data(self):
        if os.path.isfile(self.local_scored_comments_file):
            df = pd.read_parquet(self.local_scored_comments_file)
            df.index.name = 'id'
        else:
            df = pd.DataFrame()
            df.index.name = 'id'
        return df



    def score_post(self, post_id, subreddit):
        text_to_classify = self.posts_df.loc[post_id, 'title'] + " " + self.posts_df.loc[post_id, 'selftext']
        # candidate_labels = ['drivers', 'complaining', 'my state has bad drivers']
        # return_dict = self.classifier(text_to_classify, self.candidate_labels, multi_label=True)

        driver_score = self.score_text(self.candidate_labels[0], text_to_classify)
        complaining_score = self.score_text(self.candidate_labels[1], text_to_classify)
        bad_driver_score = self.score_text(self.candidate_labels[2], text_to_classify)
        # complaining_score = return_dict['scores'][1]
        # bad_driver_score = return_dict['scores'][2]
        sum_score = driver_score + complaining_score + bad_driver_score
        # bad_driving_classification = False
        # if bad_driver_score >= 0.9 and bad_driver_score > good_driver_score:
        #     bad_driving_classification = True
        # TODO: don't store text again, join and project when necessary
        post_scored_df_entry = pd.DataFrame.from_dict({
            "id":[post_id],
            "text_to_classify":[text_to_classify],
            "driving_score":[driver_score],
            "complaining_score":[complaining_score],
            "bad_driver_score":[bad_driver_score],
            "sum_score":[sum_score],
            "subreddit":[subreddit]
            # "bad_driving_classificaiton":[bad_driving_classification]
        }).set_index('id')
        # print(f"scored post {post_id} as {sum_score}")
        return post_scored_df_entry
    
    def score_comment(self, comment_id, subreddit):
        text_to_classify = self.comments_df.loc[comment_id, 'context'] + "Comment: " + self.comments_df.loc[comment_id, 'body']
        # return_dict = self.classifier(text_to_classify, self.candidate_labels, multi_label=True)
        # driver_score = return_dict['scores'][0]
        # complaining_score = return_dict['scores'][1]
        # bad_driver_score = return_dict['scores'][2]

        driver_score = self.score_text(self.candidate_labels[0], text_to_classify)
        complaining_score = self.score_text(self.candidate_labels[1], text_to_classify)
        bad_driver_score = self.score_text(self.candidate_labels[2], text_to_classify)
        sum_score = driver_score + complaining_score + bad_driver_score

        # TODO: don't store text again, join and project when necessary
        comment_scored_df_entry = pd.DataFrame.from_dict({
            "id":[comment_id],
            "text_to_classify":[text_to_classify],
            "driving_score":[driver_score],
            "complaining_score":[complaining_score],
            "bad_driver_score":[bad_driver_score],
            "sum_score":[sum_score],
            "subreddit":[subreddit]
            # "bad_driving_classificaiton":[bad_driving_classification]
        }).set_index('id')
        # print(f"scored comment {comment_id} as {sum_score}")
        return comment_scored_df_entry


# state_subreddits_df = pd.read_csv('StateSubreddits.csv')
# state_subreddits_list = state_subreddits_df['Subreddit'].to_list()
# rc = RedditCollector.RedditCollector(state_subreddits_list,'post_data.parquet', 'comments_data.parquet')
# posts_df = rc.read_local_posts_data()
# comment_df = rc.read_local_comments_data()
# a = Analyzer(posts_df, comment_df, 'scored_post_data.parquet', 'scored_comment_data.parquet')
# # a.update_score_posts(1000)
# a.update_score_comments(100)
# scored_df = a.read_local_scored_posts_data().sort_values(by=['sum_score'], ascending=False)
# scored_df = a.read_local_scored_comments_data().sort_values(by=['sum_score'], ascending=False)
# scored_df.to_csv('test_scored_comment_data.csv')
# num_posts = comment_df.value_counts("subreddit").reset_index(name='Number of Posts')
# print(scored_df)
# print(num_posts)
