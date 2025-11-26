import requests
from datetime import datetime, timedelta, timezone
from collections import defaultdict
import os
import csv
import pandas as pd
from calendar import monthrange
import time

class BitbucketChurnAnalyzer:
    def __init__(self, workspace, repo_slug, username, app_password):
        """
        Initialize the Bitbucket API client.
        
        Args:
            workspace: Bitbucket workspace ID
            repo_slug: Repository slug name
            username: Bitbucket username
            app_password: Bitbucket app password (create at: https://bitbucket.org/account/settings/app-passwords/)
        """
        self.workspace = workspace
        self.repo_slug = repo_slug
        self.auth = (username, app_password)
        self.base_url = f"https://api.bitbucket.org/2.0/repositories/{workspace}/{repo_slug}"
    
    def get_commit_diff(self, commit_hash):
        """
        Get the diff stats for a specific commit.
        
        Args:
            commit_hash: The commit hash
        
        Returns:
            Dict with lines added, removed, files changed, and commit message
        """
        url = f"{self.base_url}/diffstat/{commit_hash}"
        
        try:
            response = requests.get(url, auth=self.auth, timeout=30)
            if response.status_code != 200:
                print(f"    ⚠ Warning: Could not fetch diff for {commit_hash[:8]}: {response.status_code}")
                return None
            
            data = response.json()
            
            total_added = 0
            total_removed = 0
            files_changed = 0
            
            for file_stat in data.get('values', []):
                total_added += file_stat.get('lines_added', 0)
                total_removed += file_stat.get('lines_removed', 0)
                files_changed += 1
            
            return {
                'lines_added': total_added,
                'lines_removed': total_removed,
                'files_changed': files_changed,
                'total_churn': total_added + total_removed,
                'net_change': total_added - total_removed
            }
        except Exception as e:
            print(f"    ⚠ Error fetching diff for {commit_hash[:8]}: {str(e)}")
            return None


def process_commits_csv(input_csv, output_csv, workspace, username, app_password, delay_seconds=0.5):
    """
    Process the commits CSV file and add churn metrics for each commit.
    Reads from input CSV with commit hashes and updates with churn data.
    
    Args:
        input_csv: Path to input CSV file with commits (must have: repository_slug, commit_hash columns)
        output_csv: Path to output CSV file (will be same as input if you want to update in place)
        workspace: Bitbucket workspace
        username: Bitbucket username
        app_password: Bitbucket app password
        delay_seconds: Delay between API calls to avoid rate limiting (default: 0.5)
    """
    print(f"\n{'='*80}")
    print("PROCESSING COMMITS CSV WITH CHURN METRICS")
    print(f"{'='*80}\n")
    
    # Read the CSV file
    print(f"📖 Reading CSV file: {input_csv}")
    df = pd.read_csv(input_csv)
    
    # Check if required columns exist
    required_cols = ['repository_slug', 'commit_hash']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f"❌ Error: Missing required columns: {missing_cols}")
        print(f"Available columns: {list(df.columns)}")
        return
    
    print(f"✓ Found {len(df)} commits to process")
    print(f"Columns: {list(df.columns)}\n")
    
    # Add new columns if they don't exist
    if 'commits' not in df.columns:
        df['commits'] = 1  # Each row is 1 commit
    if 'lines_added' not in df.columns:
        df['lines_added'] = 0
    if 'lines_removed' not in df.columns:
        df['lines_removed'] = 0
    if 'total_churn' not in df.columns:
        df['total_churn'] = 0
    if 'net_change' not in df.columns:
        df['net_change'] = 0
    
    # Track stats
    total_commits = len(df)
    processed = 0
    errors = 0
    skipped = 0
    
    # Group by repository to minimize API client creation
    repos = df['repository_slug'].unique()
    print(f"📦 Processing {len(repos)} unique repositories\n")
    
    for repo_idx, repo_slug in enumerate(repos, 1):
        print(f"\n{'='*80}")
        print(f"Repository {repo_idx}/{len(repos)}: {repo_slug}")
        print(f"{'='*80}")
        
        # Create analyzer for this repository
        analyzer = BitbucketChurnAnalyzer(workspace, repo_slug, username, app_password)
        
        # Get all commits for this repository
        repo_commits = df[df['repository_slug'] == repo_slug]
        print(f"📊 Processing {len(repo_commits)} commits for {repo_slug}")
        
        for idx, row in repo_commits.iterrows():
            commit_hash = row['commit_hash']
            
            # Skip if already has data
            if pd.notna(row.get('lines_added')) and row.get('lines_added', 0) > 0:
                skipped += 1
                continue
            
            # Progress indicator
            if (processed + 1) % 10 == 0:
                print(f"  Progress: {processed + 1}/{total_commits} commits processed ({(processed + 1) / total_commits * 100:.1f}%)")
            
            try:
                # Get commit diff
                diff = analyzer.get_commit_diff(commit_hash)
                
                if diff:
                    df.at[idx, 'commits'] = 1
                    df.at[idx, 'lines_added'] = diff['lines_added']
                    df.at[idx, 'lines_removed'] = diff['lines_removed']
                    df.at[idx, 'total_churn'] = diff['total_churn']
                    df.at[idx, 'net_change'] = diff['net_change']
                    processed += 1
                else:
                    errors += 1
                    df.at[idx, 'commits'] = 1
                    df.at[idx, 'lines_added'] = 0
                    df.at[idx, 'lines_removed'] = 0
                    df.at[idx, 'total_churn'] = 0
                    df.at[idx, 'net_change'] = 0
                
                # Delay to avoid rate limiting
                if delay_seconds > 0:
                    time.sleep(delay_seconds)
                    
            except Exception as e:
                print(f"    ❌ Error processing commit {commit_hash[:8]}: {str(e)}")
                errors += 1
                continue
        
        print(f"✓ Completed {repo_slug}")
    
    # Save the updated CSV
    print(f"\n{'='*80}")
    print("SAVING RESULTS")
    print(f"{'='*80}\n")
    
    df.to_csv(output_csv, index=False, encoding='utf-8')
    
    print(f"✅ Results saved to: {output_csv}")
    print(f"\n📊 SUMMARY:")
    print(f"  Total commits:      {total_commits}")
    print(f"  Processed:          {processed}")
    print(f"  Skipped (had data): {skipped}")
    print(f"  Errors:             {errors}")
    print(f"  Success rate:       {(processed / (total_commits - skipped) * 100) if (total_commits - skipped) > 0 else 0:.1f}%")
    
    # Calculate and display aggregate stats
    print(f"\n📈 AGGREGATE METRICS:")
    print(f"  Total lines added:   {df['lines_added'].sum():,}")
    print(f"  Total lines removed: {df['lines_removed'].sum():,}")
    print(f"  Total churn:         {df['total_churn'].sum():,}")
    print(f"  Net change:          {df['net_change'].sum():,}")
    print(f"  Avg churn/commit:    {df['total_churn'].mean():.2f}")
    
    return df


def process_commits_csv_with_resume(input_csv, output_csv, workspace, username, app_password, 
                                    delay_seconds=0.5, checkpoint_interval=50):
    """
    Process the commits CSV file with checkpoint/resume capability.
    Saves progress periodically so you can resume if interrupted.
    
    Args:
        input_csv: Path to input CSV file with commits
        output_csv: Path to output CSV file
        workspace: Bitbucket workspace
        username: Bitbucket username
        app_password: Bitbucket app password
        delay_seconds: Delay between API calls (default: 0.5)
        checkpoint_interval: Save progress every N commits (default: 50)
    """
    print(f"\n{'='*80}")
    print("PROCESSING COMMITS CSV WITH CHURN METRICS (WITH RESUME)")
    print(f"{'='*80}\n")
    
    # Read the CSV file
    print(f"📖 Reading CSV file: {input_csv}")
    
    # If output exists, resume from there; otherwise start fresh
    if os.path.exists(output_csv):
        print(f"🔄 Resuming from existing output file: {output_csv}")
        df = pd.read_csv(output_csv)
    else:
        print(f"🆕 Starting fresh processing")
        df = pd.read_csv(input_csv)
    
    # Check if required columns exist
    required_cols = ['repository_slug', 'commit_hash']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f"❌ Error: Missing required columns: {missing_cols}")
        print(f"Available columns: {list(df.columns)}")
        return
    
    # Add new columns if they don't exist
    if 'commits' not in df.columns:
        df['commits'] = 1
    if 'lines_added' not in df.columns:
        df['lines_added'] = pd.NA
    if 'lines_removed' not in df.columns:
        df['lines_removed'] = pd.NA
    if 'total_churn' not in df.columns:
        df['total_churn'] = pd.NA
    if 'net_change' not in df.columns:
        df['net_change'] = pd.NA
    
    print(f"✓ Total commits in file: {len(df)}")
    
    # Count how many are already processed
    already_processed = df['lines_added'].notna().sum() and ((df['lines_added'] > 0).sum() or (df['lines_removed'] > 0).sum())
    remaining = len(df) - already_processed
    print(f"✓ Already processed: {already_processed}")
    print(f"✓ Remaining: {remaining}\n")
    
    if remaining == 0:
        print("✅ All commits already processed!")
        return df
    
    # Track stats
    total_commits = len(df)
    processed_this_run = 0
    errors = 0
    last_checkpoint = 0
    
    # Group by repository
    repos = df['repository_slug'].unique()
    print(f"📦 Processing {len(repos)} unique repositories\n")
    
    current_analyzer = None
    current_repo = None
    
    for idx, row in df.iterrows():
        # Skip if already has data
        if pd.notna(row.get('lines_added')) and (row.get('lines_added', 0) > 0 or row.get('lines_removed', 0) > 0):
            continue
        
        repo_slug = row['repository_slug']
        commit_hash = row['commit_hash']
        
        # Create new analyzer if repository changed
        if repo_slug != current_repo:
            current_repo = repo_slug
            current_analyzer = BitbucketChurnAnalyzer(workspace, repo_slug, username, app_password)
            print(f"\n📦 Switching to repository: {repo_slug}")
        
        # Progress indicator
        if (processed_this_run + 1) % 10 == 0:
            progress = (already_processed + processed_this_run) / total_commits * 100
            print(f"  Progress: {already_processed + processed_this_run}/{total_commits} ({progress:.1f}%)")
        
        try:
            # Get commit diff
            diff = current_analyzer.get_commit_diff(commit_hash)
            
            if diff:
                df.at[idx, 'commits'] = 1
                df.at[idx, 'lines_added'] = diff['lines_added']
                df.at[idx, 'lines_removed'] = diff['lines_removed']
                df.at[idx, 'total_churn'] = diff['total_churn']
                df.at[idx, 'net_change'] = diff['net_change']
                processed_this_run += 1
            else:
                errors += 1
                df.at[idx, 'commits'] = 1
                df.at[idx, 'lines_added'] = 0
                df.at[idx, 'lines_removed'] = 0
                df.at[idx, 'total_churn'] = 0
                df.at[idx, 'net_change'] = 0
            
            # Checkpoint save
            if processed_this_run - last_checkpoint >= checkpoint_interval:
                print(f"  💾 Checkpoint: Saving progress...")
                df.to_csv(output_csv, index=False, encoding='utf-8')
                last_checkpoint = processed_this_run
            
            # Delay to avoid rate limiting
            if delay_seconds > 0:
                time.sleep(delay_seconds)
                
        except Exception as e:
            print(f"    ❌ Error processing commit {commit_hash[:8]}: {str(e)}")
            errors += 1
            continue
    
    # Final save
    print(f"\n{'='*80}")
    print("SAVING FINAL RESULTS")
    print(f"{'='*80}\n")
    
    df.to_csv(output_csv, index=False, encoding='utf-8')
    
    print(f"✅ Results saved to: {output_csv}")
    print(f"\n📊 SUMMARY:")
    print(f"  Total commits:           {total_commits}")
    print(f"  Already processed:       {already_processed}")
    print(f"  Processed this run:      {processed_this_run}")
    print(f"  Errors:                  {errors}")
    print(f"  Total now processed:     {already_processed + processed_this_run}")
    
    # Calculate and display aggregate stats
    print(f"\n📈 AGGREGATE METRICS:")
    total_added = df['lines_added'].sum()
    total_removed = df['lines_removed'].sum()
    total_churn_sum = df['total_churn'].sum()
    net_change_sum = df['net_change'].sum()
    avg_churn = df['total_churn'].mean()
    
    print(f"  Total lines added:   {total_added:,}")
    print(f"  Total lines removed: {total_removed:,}")
    print(f"  Total churn:         {total_churn_sum:,}")
    print(f"  Net change:          {net_change_sum:,}")
    print(f"  Avg churn/commit:    {avg_churn:.2f}")
    
    return df


if __name__ == "__main__":
    # Configuration
    BITBUCKET_WORKSPACE = os.getenv("BITBUCKET_WORKSPACE")
    USERNAME = os.getenv("BITBUCKET_USERNAME", "")
    APP_PASSWORD = os.getenv("BITBUCKET_API_TOKEN", "")
    
    # File paths
    INPUT_CSV = "bitbucket_commits_merged_allcommits.csv"
    OUTPUT_CSV = "bitbucket_commits_merged_allcommits.csv"  # Same file to update in place
    
    # Processing settings
    DELAY_SECONDS = 0.5  # Delay between API calls to avoid rate limiting
    CHECKPOINT_INTERVAL = 50  # Save progress every N commits
    
    # Run processing with resume capability
    print("="*80)
    print("BITBUCKET COMMIT CHURN ANALYSIS")
    print("="*80)
    print(f"Input file:  {INPUT_CSV}")
    print(f"Output file: {OUTPUT_CSV}")
    print(f"Workspace:   {BITBUCKET_WORKSPACE}")
    print(f"Delay:       {DELAY_SECONDS}s between requests")
    print(f"Checkpoint:  Every {CHECKPOINT_INTERVAL} commits")
    print("="*80)
    
    if not USERNAME or not APP_PASSWORD:
        print("\n❌ Error: Please set BITBUCKET_USERNAME and BITBUCKET_API_TOKEN environment variables")
        print("\nExample:")
        print("  export BITBUCKET_USERNAME='your_username'")
        print("  export BITBUCKET_API_TOKEN='your_app_password'")
        exit(1)
    
    df = process_commits_csv_with_resume(
        input_csv=INPUT_CSV,
        output_csv=OUTPUT_CSV,
        workspace=BITBUCKET_WORKSPACE,
        username=USERNAME,
        app_password=APP_PASSWORD,
        delay_seconds=DELAY_SECONDS,
        checkpoint_interval=CHECKPOINT_INTERVAL
    )
    
    print("\n✅ Processing complete!")
