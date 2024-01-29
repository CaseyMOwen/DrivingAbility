from transformers import pipeline
import pandas as pd
import RedditCollector
import os.path
import torch
from transformers import AutoModelForSequenceClassification, AutoTokenizer
import numpy as np
from sklearn import preprocessing

class Analyzer():
    def __init__(self, posts_df, comments_df, local_scored_posts_file, local_scored_comments_file):
        self.posts_df = posts_df
        self.comments_df = comments_df
        # self.classifier = pipeline("zero-shot-classification",
                    #   model="facebook/bart-large-mnli")
        self.local_scored_posts_file = local_scored_posts_file
        self.local_scored_comments_file = local_scored_comments_file
        self.labels = {
            'drivers': 'pos',
            'complaining': 'pos',
            'license':'neg',
            'idiot': 'pos',
            'DMV':'neg',
            'ticket':'neg',
            'insurance':'neg',
            'politics':'neg',
            'weather':'neg',
            'conditions': 'neg',
            'gas':'neg',
        }
        self.tokenizer = AutoTokenizer.from_pretrained('facebook/bart-large-mnli')
        self.device = 'cuda'
        with torch.device(self.device):
            self.nli_model = AutoModelForSequenceClassification.from_pretrained('facebook/bart-large-mnli', torch_dtype=torch.float16)
        
        
    def score_text(self, label, text):
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
            # subreddit = self.posts_df.loc[post_id, 'subreddit']
            post_classified_df_entry = self.score_post(post_id)
            post_classified_df_entries.append(post_classified_df_entry)
            count += 1
        full_df = pd.concat(post_classified_df_entries)
        merged_df = pd.concat([full_df, existing_scored_df])
        merged_df = merged_df[~merged_df.index.duplicated(keep='first')]
        min_max_scaler = preprocessing.MinMaxScaler()
        # merged_df['pos_score_norm'] = min_max_scaler.fit_transform(merged_df['pos_score'])
        # merged_df = merged_df.assign(pos_score_norm=pd.Series(min_max_scaler.fit_transform(merged_df['pos_score'])).values)
        # merged_df = merged_df.assign(neg_score_norm=pd.Series(min_max_scaler.fit_transform(merged_df['neg_score'])).values)

        merged_df['pos_score_norm'] = min_max_scaler.fit_transform(merged_df[['pos_score']])
        merged_df['neg_score_norm'] = min_max_scaler.fit_transform(merged_df[['neg_score']])
        # merged_df['neg_score_norm'] = min_max_scaler.fit_transform(merged_df['neg_score'])
        merged_df["tot_score"] = merged_df["pos_score_norm"] - merged_df["neg_score_norm"]
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
            # subreddit = self.comments_df.loc[comment_id, 'subreddit']
            comment_classified_df_entry = self.score_comment(comment_id)
            comment_classified_df_entries.append(comment_classified_df_entry)
            count += 1
        full_df = pd.concat(comment_classified_df_entries)
        merged_df = pd.concat([full_df, existing_scored_df])
        merged_df = merged_df[~merged_df.index.duplicated(keep='first')]
        min_max_scaler = preprocessing.MinMaxScaler()
        merged_df['pos_score_norm'] = min_max_scaler.fit_transform(merged_df[['pos_score']])
        merged_df['neg_score_norm'] = min_max_scaler.fit_transform(merged_df[['neg_score']])
        # merged_df['pos_score_norm'] = min_max_scaler.fit_transform(merged_df['pos_score'])
        # merged_df['neg_score_norm'] = min_max_scaler.fit_transform(merged_df['neg_score'])

        # merged_df = merged_df.assign(pos_score_norm=pd.Series(min_max_scaler.fit_transform(merged_df['pos_score'])).values)
        # merged_df = merged_df.assign(neg_score_norm=pd.Series(min_max_scaler.fit_transform(merged_df['neg_score'])).values)
        merged_df["tot_score"] = merged_df["pos_score_norm"] - merged_df["neg_score_norm"]
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

    def get_scores(self, text_to_classify):
        # candidate_labels = ['drivers', 'complaining', 'license, registration, DMV', 'politics', 'complaining about drivers in their state']
        label_scores = {}
        pos_score = 0
        neg_score = 0
        for label in self.labels:
            score = self.score_text(label, text_to_classify)
            label_scores[label] = score
            if self.labels[label] == 'pos':
                pos_score += score
            elif self.labels[label] == 'neg':
                neg_score += score
        return label_scores, pos_score, neg_score

    
    def score_post(self, post_id):
        text_to_classify = self.posts_df.loc[post_id, 'title'] + " " + self.posts_df.loc[post_id, 'selftext']
        label_scores, pos_score, neg_score = self.get_scores(text_to_classify)
        df_entry_dict = {
            "id":[post_id],
            "pos_score":[pos_score],
            "neg_score":[neg_score],
        }
        for label in label_scores:
            col_name = label + '_score'
            df_entry_dict[col_name] = label_scores[label]
        post_scored_df_entry = pd.DataFrame.from_dict(df_entry_dict).set_index('id')
        return post_scored_df_entry
    
    def score_comment(self, comment_id):
        text_to_classify = self.comments_df.loc[comment_id, 'context'] + "Comment: " + self.comments_df.loc[comment_id, 'body']
        label_scores, pos_score, neg_score = self.get_scores(text_to_classify)
        df_entry_dict = {
            "id":[comment_id],
            "pos_score":[pos_score],
            "neg_score":[neg_score],
        }
        for label in label_scores:
            col_name = label + '_score'
            df_entry_dict[col_name] = label_scores[label]
        
        comment_scored_df_entry = pd.DataFrame.from_dict(df_entry_dict).set_index('id')
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
