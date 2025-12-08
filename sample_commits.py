import pandas as pd
import random

# Read CSVs
gitlab_df = pd.read_csv('glab.csv')
bitbucket_df = pd.read_csv('bitbucket.csv')

# Parse dates
gitlab_df['created_at'] = pd.to_datetime(gitlab_df['created_at'], utc=True)
bitbucket_df['date'] = pd.to_datetime(bitbucket_df['date'], utc=True)

# Cutoff
cutoff = pd.to_datetime('2024-10-01', utc=True)

# Split groups
gitlab_pre = gitlab_df[gitlab_df['created_at'] <= cutoff]
gitlab_post = gitlab_df[gitlab_df['created_at'] > cutoff]
bitbucket_pre = bitbucket_df[bitbucket_df['date'] <= cutoff]
bitbucket_post = bitbucket_df[bitbucket_df['date'] > cutoff]

# Sample 50 from each per group
def sample(df, n):
    if len(df) < n:
        return df
    return df.sample(n=n, random_state=42)

gitlab_pre_sample = sample(gitlab_pre, 50)
gitlab_post_sample = sample(gitlab_post, 50)
bitbucket_pre_sample = sample(bitbucket_pre, 50)
bitbucket_post_sample = sample(bitbucket_post, 50)

# Add columns
gitlab_pre_sample = gitlab_pre_sample.copy()
gitlab_post_sample = gitlab_post_sample.copy()
bitbucket_pre_sample = bitbucket_pre_sample.copy()
bitbucket_post_sample = bitbucket_post_sample.copy()

gitlab_pre_sample['platform'] = 'GitLab'
gitlab_pre_sample['date_group'] = 'pre'
gitlab_post_sample['platform'] = 'GitLab'
gitlab_post_sample['date_group'] = 'post'
bitbucket_pre_sample['platform'] = 'Bitbucket'
bitbucket_pre_sample['date_group'] = 'pre'
bitbucket_post_sample['platform'] = 'Bitbucket'
bitbucket_post_sample['date_group'] = 'post'

# Combine
combined = pd.concat([gitlab_pre_sample, gitlab_post_sample, bitbucket_pre_sample, bitbucket_post_sample], ignore_index=True)

# Save to CSV
combined.to_csv('sampled_commits_200.csv', index=False)

print("Sampled commits saved to sampled_commits_200.csv")
