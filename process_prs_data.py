import pandas as pd
import json
from datetime import datetime
from pathlib import Path
import numpy as np
import argparse

def parse_date(date_str):
    """Parse date string to datetime object"""
    if pd.isna(date_str):
        return None
    try:
        # Try different date formats
        for fmt in ['%Y-%m-%d %H:%M:%S%z', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d']:
            try:
                return pd.to_datetime(date_str, format=fmt)
            except:
                continue
        return pd.to_datetime(date_str)
    except:
        return None

def is_in_date_range(date_obj):
    """Check if date is in the specified ranges"""
    if date_obj is None:
        return False
    
    # Convert to naive datetime for comparison
    if date_obj.tzinfo is not None:
        date_obj = date_obj.replace(tzinfo=None)
    
    # Range 1: 2024-07-01 to 2024-10-01
    range1_start = datetime(2024, 7, 1)
    range1_end = datetime(2024, 10, 1)
    
    # Range 2: 2025-07-01 to 2025-10-01
    range2_start = datetime(2025, 7, 1)
    range2_end = datetime(2025, 10, 1)
    
    return (range1_start <= date_obj < range1_end) or (range2_start <= date_obj < range2_end)

def get_month_year(date_obj):
    """Get month-year string in MM-YYYY format"""
    if date_obj is None:
        return None
    if date_obj.tzinfo is not None:
        date_obj = date_obj.replace(tzinfo=None)
    return date_obj.strftime('%m-%Y')

def get_period(date_obj):
    """Get period identifier (2024 or 2025)"""
    if date_obj is None:
        return None
    if date_obj.tzinfo is not None:
        date_obj = date_obj.replace(tzinfo=None)
    return date_obj.year

def get_common_participants(df, author_col, reviewer_col):
    """
    Get common participants (authors and reviewers) between 2024 and 2025 periods
    Returns a set of common participant names
    """
    df['period'] = df['created_date'].apply(get_period)
    
    # Get all participants per period
    participants_2024 = set()
    participants_2025 = set()
    
    for _, row in df.iterrows():
        period = row['period']
        if period is None:
            continue
        
        # Add author
        if pd.notna(row[author_col]):
            author = str(row[author_col]).strip()
            if author:
                if period == 2024:
                    participants_2024.add(author)
                elif period == 2025:
                    participants_2025.add(author)
        
        # Add reviewers
        if pd.notna(row[reviewer_col]):
            reviewers_str = str(row[reviewer_col])
            reviewers = [r.strip() for r in reviewers_str.split(',') if r.strip()]
            for reviewer in reviewers:
                if period == 2024:
                    participants_2024.add(reviewer)
                elif period == 2025:
                    participants_2025.add(reviewer)
    
    common = participants_2024 & participants_2025
    print(f"  Participants in 2024: {len(participants_2024)}")
    print(f"  Participants in 2025: {len(participants_2025)}")
    print(f"  Common participants: {len(common)}")
    
    return common

def is_common_participant(row, common_participants, author_col, reviewer_col):
    """Check if PR involves at least one common participant (as author or reviewer)"""
    if not common_participants:
        return True  # If no filtering, include all
    
    # Check author
    if pd.notna(row[author_col]):
        author = str(row[author_col]).strip()
        if author in common_participants:
            return True
    
    # Check reviewers
    if pd.notna(row[reviewer_col]):
        reviewers_str = str(row[reviewer_col])
        reviewers = [r.strip() for r in reviewers_str.split(',') if r.strip()]
        if any(r in common_participants for r in reviewers):
            return True
    
    return False

def process_gitlab_data(csv_path, common_only=False):
    """Process GitLab MRs data"""
    print(f"Processing GitLab data from: {csv_path}")
    
    df = pd.read_csv(csv_path)
    
    # Parse dates
    df['created_date'] = df['created_at'].apply(parse_date)
    df['merged_date'] = df['merged_at'].apply(parse_date)
    df['updated_date'] = df['updated_at'].apply(parse_date)
    
    # Filter by date range based on created_at
    df = df[df['created_date'].apply(is_in_date_range)].copy()
    
    print(f"GitLab: {len(df)} MRs in date range")
    
    # Get common participants if needed
    common_participants = set()
    if common_only:
        print("GitLab: Filtering for common participants only")
        common_participants = get_common_participants(df, 'anonymized_name', 'anonymized_reviewers')
        # Filter dataframe
        df = df[df.apply(lambda row: is_common_participant(row, common_participants, 
                                                           'anonymized_name', 'anonymized_reviewers'), axis=1)].copy()
        print(f"GitLab: {len(df)} MRs after common participants filter")
    
    # Convert duration_hours to numeric
    df['duration_hours'] = pd.to_numeric(df['duration_hours'], errors='coerce')
    
    # Calculate month-year
    df['month_year'] = df['created_date'].apply(get_month_year)
    
    # Prepare list data
    list_data = []
    for _, row in df.iterrows():
        date_obj = row['created_date']
        if date_obj is None:
            continue
            
        is_merged = row['state'] == 'merged'
        
        # Parse churn data
        files_changed = int(row['files_changed']) if pd.notna(row['files_changed']) else 0
        lines_added = int(row['lines_added']) if pd.notna(row['lines_added']) else 0
        lines_deleted = int(row['lines_deleted']) if pd.notna(row['lines_deleted']) else 0
        
        # Parse reviewers_count - handle both numeric and string values
        reviewers_count = 0
        if pd.notna(row.get('reviewers_count')):
            try:
                reviewers_count = int(row['reviewers_count'])
            except (ValueError, TypeError):
                # If conversion fails, count from reviewers list
                if pd.notna(row.get('anonymized_reviewers')):
                    reviewers_str = str(row['anonymized_reviewers'])
                    if reviewers_str and reviewers_str != '':
                        reviewers_count = len([r.strip() for r in reviewers_str.split(',') if r.strip()])
        
        item = {
            "date": date_obj.strftime('%Y-%m-%d'),
            "created": 1,
            "merged": 1 if is_merged else 0,
            "reviewTimeInHours": float(row['duration_hours']) if pd.notna(row['duration_hours']) else None,
            "mergeTime": float(row['duration_hours']) if is_merged and pd.notna(row['duration_hours']) else None,
            "reviewers": 0.0,
            "authors": row['anonymized_name'] if pd.notna(row['anonymized_name']) else "",
            "reviewersList": row['anonymized_reviewers'] if pd.notna(row['anonymized_reviewers']) else "",
            "reviewersCount": reviewers_count,
            "churn": {
                "filesChanged": files_changed,
                "codeChurn": lines_added + lines_deleted,
                "netChange": lines_added - lines_deleted
            }
        }
        list_data.append(item)
    
    # Calculate summary by month
    summary_by_month = {}
    for month_year in df['month_year'].unique():
        if pd.isna(month_year):
            continue
            
        month_data = df[df['month_year'] == month_year].copy()
        merged_data = month_data[month_data['state'] == 'merged']
        
        # Calculate metrics
        duration_values = month_data['duration_hours'].dropna()
        merged_duration_values = merged_data['duration_hours'].dropna()
        
        summary_by_month[month_year] = {
            "avgReviewTimeInHours": float(duration_values.mean()) if len(duration_values) > 0 else 0,
            "medianReviewTimeInHours": float(duration_values.median()) if len(duration_values) > 0 else 0,
            "avgMergeTimeInHours": float(merged_duration_values.mean()) if len(merged_duration_values) > 0 else 0,
            "medianMergeTimeInHours": float(merged_duration_values.median()) if len(merged_duration_values) > 0 else 0,
            "totalPrs": int(len(month_data)),
            "totalMerged": int(len(merged_data))
        }
    
    return {
        "prData": {
            "summaryByMonth": summary_by_month,
            "list": list_data
        }
    }

def process_bitbucket_data(csv_path, common_only=False):
    """Process Bitbucket PRs data"""
    print(f"Processing Bitbucket data from: {csv_path}")
    
    df = pd.read_csv(csv_path)
    
    # Parse dates
    df['created_date'] = df['created_on'].apply(parse_date)
    df['updated_date'] = df['updated_on'].apply(parse_date)
    
    # Filter by date range based on created_on
    df = df[df['created_date'].apply(is_in_date_range)].copy()
    
    print(f"Bitbucket: {len(df)} PRs in date range")
    
    # Get common participants if needed
    common_participants = set()
    if common_only:
        print("Bitbucket: Filtering for common participants only")
        common_participants = get_common_participants(df, 'anonymized_author', 'anonymized_reviewers')
        # Filter dataframe
        df = df[df.apply(lambda row: is_common_participant(row, common_participants, 
                                                           'anonymized_author', 'anonymized_reviewers'), axis=1)].copy()
        print(f"Bitbucket: {len(df)} PRs after common participants filter")
    
    # Convert cycle_time_hours to numeric
    df['cycle_time_hours'] = pd.to_numeric(df['cycle_time_hours'], errors='coerce')
    
    # Calculate month-year
    df['month_year'] = df['created_date'].apply(get_month_year)
    
    # Prepare list data
    list_data = []
    for _, row in df.iterrows():
        date_obj = row['created_date']
        if date_obj is None:
            continue
            
        is_merged = row['pr_state'] == 'MERGED'
        
        # Check if churn columns exist
        files_changed = 0
        lines_added = 0
        lines_deleted = 0
        
        if 'files_changed' in df.columns and pd.notna(row.get('files_changed')):
            files_changed = int(row['files_changed'])
        if 'lines_added' in df.columns and pd.notna(row.get('lines_added')):
            lines_added = int(row['lines_added'])
        if 'lines_deleted' in df.columns and pd.notna(row.get('lines_deleted')):
            lines_deleted = int(row['lines_deleted'])
        
        # Count reviewers
        reviewers_count = 0
        if pd.notna(row.get('anonymized_reviewers')):
            reviewers_str = str(row['anonymized_reviewers'])
            if reviewers_str and reviewers_str != '':
                reviewers_count = len([r.strip() for r in reviewers_str.split(',') if r.strip()])
        
        item = {
            "date": date_obj.strftime('%Y-%m-%d'),
            "created": 1,
            "merged": 1 if is_merged else 0,
            "reviewTimeInHours": float(row['cycle_time_hours']) if pd.notna(row['cycle_time_hours']) else None,
            "mergeTime": float(row['cycle_time_hours']) if is_merged and pd.notna(row['cycle_time_hours']) else None,
            "reviewers": 0.0,
            "authors": row['anonymized_author'] if pd.notna(row['anonymized_author']) else "",
            "reviewersList": row['anonymized_reviewers'] if pd.notna(row['anonymized_reviewers']) else "",
            "reviewersCount": reviewers_count,
            "churn": {
                "filesChanged": files_changed,
                "codeChurn": lines_added + lines_deleted,
                "netChange": lines_added - lines_deleted
            }
        }
        list_data.append(item)
    
    # Calculate summary by month
    summary_by_month = {}
    for month_year in df['month_year'].unique():
        if pd.isna(month_year):
            continue
            
        month_data = df[df['month_year'] == month_year].copy()
        merged_data = month_data[month_data['pr_state'] == 'MERGED']
        
        # Calculate metrics
        cycle_values = month_data['cycle_time_hours'].dropna()
        merged_cycle_values = merged_data['cycle_time_hours'].dropna()
        
        summary_by_month[month_year] = {
            "avgReviewTimeInHours": float(cycle_values.mean()) if len(cycle_values) > 0 else 0,
            "medianReviewTimeInHours": float(cycle_values.median()) if len(cycle_values) > 0 else 0,
            "avgMergeTimeInHours": float(merged_cycle_values.mean()) if len(merged_cycle_values) > 0 else 0,
            "medianMergeTimeInHours": float(merged_cycle_values.median()) if len(merged_cycle_values) > 0 else 0,
            "totalPrs": int(len(month_data)),
            "totalMerged": int(len(merged_data))
        }
    
    return {
        "prData": {
            "summaryByMonth": summary_by_month,
            "list": list_data
        }
    }

def main():
    """Main function to process both CSVs and generate JSON"""
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Process PRs data from GitLab and Bitbucket')
    parser.add_argument('--common-only', action='store_true', 
                       help='Filter metrics to include only common participants between 2024 and 2025 periods')
    args = parser.parse_args()
    
    # Define paths
    base_dir = Path('/home/osw100337/Documents/DevEx')
    gitlab_csv = base_dir / 'consolidated' / 'gitlab_mrs_merged_20251024_114758.csv'
    bitbucket_csv = base_dir / 'consolidated' / 'bitbucket_prs_merged_20251024_114911.csv'
    
    # Adjust output filename based on filter
    if args.common_only:
        output_json = base_dir / 'prs_metrics_output_common_only.json'
        print("\n=== Processing with COMMON PARTICIPANTS ONLY filter ===\n")
    else:
        output_json = base_dir / 'prs_metrics_output.json'
        print("\n=== Processing ALL participants ===\n")
    
    # Process data
    result = {}
    
    if gitlab_csv.exists():
        result['gitlab'] = process_gitlab_data(gitlab_csv, common_only=args.common_only)
    else:
        print(f"Warning: GitLab CSV not found at {gitlab_csv}")
        result['gitlab'] = {"prData": {"summaryByMonth": {}, "list": []}}
    
    if bitbucket_csv.exists():
        result['bitbucket'] = process_bitbucket_data(bitbucket_csv, common_only=args.common_only)
    else:
        print(f"Warning: Bitbucket CSV not found at {bitbucket_csv}")
        result['bitbucket'] = {"prData": {"summaryByMonth": {}, "list": []}}
    
    # Save to JSON
    with open(output_json, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    
    print(f"\nOutput saved to: {output_json}")
    print(f"GitLab PRs processed: {len(result['gitlab']['prData']['list'])}")
    print(f"GitLab months: {len(result['gitlab']['prData']['summaryByMonth'])}")
    print(f"Bitbucket PRs processed: {len(result['bitbucket']['prData']['list'])}")
    print(f"Bitbucket months: {len(result['bitbucket']['prData']['summaryByMonth'])}")

if __name__ == "__main__":
    main()