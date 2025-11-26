#!/usr/bin/env python3
"""
Mann-Whitney U Test Analysis for DevEx Metrics Research Questions
Analyzes pre/post intervention data for RQ1 (Feedback Loops), RQ2 (Cognitive Load), and RQ3 (Flow State)

Usage:
    python mann_whitney_analysis.py --input consolidated_metrics.json --reference-date 2024-10-08
    python mann_whitney_analysis.py --input-csv data.csv --reference-date 2024-10-08 --mode csv
    
    # Extract table data from results
    python mann_whitney_analysis.py --extract-table mw_bitbucket.json mW_gitlab.json
"""

import pandas as pd
import json
import argparse
import numpy as np
from scipy import stats
from datetime import datetime
import sys
from pathlib import Path

def extract_table_data(bitbucket_json, gitlab_json, output_csv='table_data.csv'):
    """
    Extract data from Mann-Whitney results to fill LaTeX table
    
    Args:
        bitbucket_json: Path to Bitbucket results JSON
        gitlab_json: Path to GitLab results JSON
        output_csv: Output CSV file for table data
    """
    print("\n" + "="*70)
    print("📊 EXTRACTING TABLE DATA FROM MANN-WHITNEY RESULTS")
    print("="*70)
    
    # Load results
    with open(bitbucket_json, 'r') as f:
        bb_data = json.load(f)
    with open(gitlab_json, 'r') as f:
        gl_data = json.load(f)
    
    # Metric mappings (result key -> display name)
    metrics = {
        'Pipeline Metrics': {
            'pipelineExecutionFrequency_full': 'Pipeline Exec. Freq. (per week)',
            'pipelineSuccessRate_full': 'Pipeline Success Rate (weekly %)',
            'buildDuration': 'Pipeline Duration (min)',
        },
        'MR/PR Metrics': {
            'mrCreationRate_full': 'MR/PR Creation Rate (per week)',
            'mrReviewTime_full': 'MR/PR Review Time (hours)',
            'mrMergeTime_full': 'MR/PR Merge Time (hours)',
            'codeReviewParticipation_full': 'Code Review Participation (reviewers)',
        },
        'Commit Metrics': {
            'commitFrequency_full': 'Commit Frequency (per week)',
            'commitLevelChurn_full': 'Code Churn (commit-level)',
            'commitMessageLength': 'Commit Message Length (chars)',
        },
        'Churn Metrics': {
            'mrLevelChurn_full': 'Code Churn (MR-level)',
        },
        'Jira Metrics': {
            'issueCycleTime': 'Issue Cycle Time (hours)',
            'operationalTicketVolume': 'Operational Ticket Volume (per week)',
        },
        'Developer Productivity': {
            'commitsPerDeveloper_full': 'Commits per Developer (per week)',
            'mrsPerDeveloper_full': 'MRs per Developer (per week)',
        }
    }
    
    # Extract data
    table_rows = []
    
    for section, section_metrics in metrics.items():
        table_rows.append({
            'Section': section,
            'Metric': '',
            'GL_2024': '',
            'GL_2025': '',
            'GL_Delta': '',
            'GL_n1': '',
            'GL_n2': '',
            'BB_2024': '',
            'BB_2025': '',
            'BB_Delta': '',
            'BB_n1': '',
            'BB_n2': '',
            'GL_pValue': '',
            'BB_pValue': ''
        })
        
        for key, display_name in section_metrics.items():
            # Find the metric in RQ sections
            gl_metric = None
            bb_metric = None
            
            for rq in ['rq1_feedback_loops', 'rq2_cognitive_load', 'rq3_flow_state']:
                if key in gl_data.get(rq, {}):
                    gl_metric = gl_data[rq][key]
                if key in bb_data.get(rq, {}):
                    bb_metric = bb_data[rq][key]
            
            row = {
                'Section': '',
                'Metric': display_name,
                'GL_2024': round(gl_metric.get('medianPre', 0), 2) if gl_metric else 'N/A',
                'GL_2025': round(gl_metric.get('medianPost', 0), 2) if gl_metric else 'N/A',
                'GL_Delta': round(gl_metric.get('percentageChange', 0), 1) if gl_metric else 'N/A',
                'GL_n1': gl_metric.get('n1', 0) if gl_metric else 'N/A',
                'GL_n2': gl_metric.get('n2', 0) if gl_metric else 'N/A',
                'BB_2024': round(bb_metric.get('medianPre', 0), 2) if bb_metric else 'N/A',
                'BB_2025': round(bb_metric.get('medianPost', 0), 2) if bb_metric else 'N/A',
                'BB_Delta': round(bb_metric.get('percentageChange', 0), 1) if bb_metric else 'N/A',
                'BB_n1': bb_metric.get('n1', 0) if bb_metric else 'N/A',
                'BB_n2': bb_metric.get('n2', 0) if bb_metric else 'N/A',
                'GL_pValue': f"{gl_metric.get('pValue', 1):.4f}" if gl_metric else 'N/A',
                'BB_pValue': f"{bb_metric.get('pValue', 1):.4f}" if bb_metric else 'N/A'
            }
            
            table_rows.append(row)
    
    # Create DataFrame
    df = pd.DataFrame(table_rows)
    df.to_csv(output_csv, index=False)
    
    print(f"\n✅ Table data extracted to: {output_csv}")
    print("\nPreview:")
    print(df.to_string(index=False))
    
    # Also create a LaTeX-ready format
    latex_file = output_csv.replace('.csv', '_latex.txt')
    with open(latex_file, 'w') as f:
        for _, row in df.iterrows():
            if row['Section']:
                f.write(f"\\midrule\n\\multicolumn{{7}}{{l}}{{\\textit{{{row['Section']}}}}} \\\\\n\\midrule\n")
            else:
                f.write(f"{row['Metric']} & {row['GL_2024']} & {row['GL_2025']} & {row['GL_Delta']} & {row['BB_2024']} & {row['BB_2025']} & {row['BB_Delta']} \\\\\n")
    
    print(f"\n✅ LaTeX format saved to: {latex_file}")
    
    return df

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Perform Mann-Whitney U tests for DevEx metrics research questions',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # From JSON metrics file
  python mann_whitney_analysis.py --input consolidated_metrics.json --reference-date 2024-10-08
  
  # From CSV files
  python mann_whitney_analysis.py --input-csv data.csv --reference-date 2024-10-08 --mode csv
  
  # Extract table data
  python mann_whitney_analysis.py --extract-table mw_bitbucket.json mW_gitlab.json
  
  # Specify output file
  python mann_whitney_analysis.py --input metrics.json --reference-date 2024-10-08 --output rq_analysis.json
  
  # Include detailed statistics
  python mann_whitney_analysis.py --input metrics.json --reference-date 2024-10-08 --verbose
        """
    )
    
    parser.add_argument('--input', type=str,
                        help='Input JSON file with consolidated metrics')
    parser.add_argument('--input-csv', type=str,
                        help='Input CSV file with raw data')
    parser.add_argument('--commits-csv', type=str,
                        help='CSV file with commit data')
    parser.add_argument('--mrs-csv', type=str,
                        help='CSV file with MR/PR data')
    parser.add_argument('--pipelines-csv', type=str,
                        help='CSV file with pipeline data')
    parser.add_argument('--jira-csv', type=str,
                        help='CSV file with Jira data')
    parser.add_argument('--copilot-csv', type=str,
                        help='CSV file with Copilot metrics data')
    parser.add_argument('--churn-csv', type=str,
                        help='CSV file with churn data from MR/PR and commits')
    parser.add_argument('--pr-churn-csv', type=str,
                        help='CSV file with precomputed PR/MR churn metrics (e.g. consolidated/churn_results/pr_churn_bitbucket.csv)')
    parser.add_argument('--commit-churn-csv', type=str,
                        help='CSV file with precomputed commit churn metrics (e.g. consolidated/churn_results/commit_churn_bitbucket.csv)')
    parser.add_argument('--reference-date', type=str, required=True,
                        help='Reference date to split pre/post periods (YYYY-MM-DD)')
    parser.add_argument('--mode', type=str, choices=['json', 'csv'], default='json',
                        help='Input mode: json or csv (default: json)')
    parser.add_argument('--output', type=str, default='mann_whitney_results.json',
                        help='Output JSON file (default: mann_whitney_results.json)')
    parser.add_argument('--verbose', action='store_true',
                        help='Print detailed statistics')
    parser.add_argument('--workforce-mode', type=str, 
                        choices=['full', 'common', 'both'], default='both',
                        help='Workforce analysis mode: full, common, or both (default: both)')
    parser.add_argument('--extract-table', nargs=2, metavar=('BITBUCKET_JSON', 'GITLAB_JSON'),
                        help='Extract table data from Mann-Whitney results (Bitbucket and GitLab JSON files)')
    parser.add_argument('--table-output', type=str, default='table_data.csv',
                        help='Output CSV file for extracted table data (default: table_data.csv)')
    
    return parser.parse_args()

def perform_mann_whitney(pre_group, post_group, metric_name, common_contributors=None, 
                         all_contributors_pre=None, all_contributors_post=None):
    """
    Perform Mann-Whitney U test and calculate effect size
    
    Args:
        pre_group: Pre-intervention data
        post_group: Post-intervention data
        metric_name: Name of the metric being tested
        common_contributors: List of contributors present in both periods
        all_contributors_pre: All contributors in pre period
        all_contributors_post: All contributors in post period
    
    Returns:
        dict: Test results including statistic, p-value, effect size
    """
    try:
        # Remove NaN values
        pre_group = np.array(pre_group)
        post_group = np.array(post_group)
        pre_group = pre_group[~np.isnan(pre_group)]
        post_group = post_group[~np.isnan(post_group)]
        
        if len(pre_group) == 0 or len(post_group) == 0:
            return {
                "metric": metric_name,
                "error": "Insufficient data",
                "n1": int(len(pre_group)),
                "n2": int(len(post_group))
            }
        
        statistic, p_value = stats.mannwhitneyu(
            pre_group, 
            post_group, 
            alternative='two-sided'
        )
        
        # Calculate effect size (r = Z / sqrt(N))
        n1, n2 = len(pre_group), len(post_group)
        z_score = stats.norm.ppf(1 - p_value/2) if p_value > 0 else 0
        effect_size = abs(z_score) / np.sqrt(n1 + n2) if (n1 + n2) > 0 else 0
        
        # Calculate medians and other statistics
        median_pre = float(np.median(pre_group))
        median_post = float(np.median(post_group))
        mean_pre = float(np.mean(pre_group))
        mean_post = float(np.mean(post_group))
        std_pre = float(np.std(pre_group, ddof=1)) if len(pre_group) > 1 else 0
        std_post = float(np.std(post_group, ddof=1)) if len(post_group) > 1 else 0
        
        # Effect size interpretation (Cohen's conventions)
        if effect_size < 0.1:
            size_interpretation = "negligible"
        elif effect_size < 0.3:
            size_interpretation = "small"
        elif effect_size < 0.5:
            size_interpretation = "medium"
        else:
            size_interpretation = "large"
        
        # Calculate percentage change
        percentage_change = ((median_post - median_pre) / median_pre * 100) if median_pre != 0 else 0
        
        result = {
            "metric": metric_name,
            "statistic": float(statistic),
            "pValue": float(p_value),
            "significant": bool(p_value < 0.05),
            "effectSize": float(effect_size),
            "effectSizeInterpretation": size_interpretation,
            "n1": int(n1),
            "n2": int(n2),
            "medianPre": median_pre,
            "medianPost": median_post,
            "meanPre": mean_pre,
            "meanPost": mean_post,
            "stdPre": std_pre,
            "stdPost": std_post,
            "percentageChange": round(percentage_change, 2)
        }
        
        # Add contributor information if available
        if common_contributors is not None:
            result["commonContributors"] = common_contributors
            result["commonContributorsCount"] = len(common_contributors)
        if all_contributors_pre is not None:
            result["allContributorsPre"] = all_contributors_pre
            result["allContributorsPreCount"] = len(all_contributors_pre)
        if all_contributors_post is not None:
            result["allContributorsPost"] = all_contributors_post
            result["allContributorsPostCount"] = len(all_contributors_post)
        
        return result
    except Exception as e:
        return {
            "metric": metric_name,
            "error": str(e)
        }

def split_by_reference_date(df, date_column, reference_date):
    """
    Split dataframe into pre and post periods based on reference date
    
    Args:
        df: DataFrame to split
        date_column: Name of the date column
        reference_date: Reference date to split on
    
    Returns:
        tuple: (pre_df, post_df)
    """
    # Work on a copy to avoid modifying caller's dataframe in-place

    df = df.copy()

    # Parse the dataframe date column as timezone-aware UTC timestamps to avoid
    # comparisons between tz-naive and tz-aware datetimes.
    df[date_column] = pd.to_datetime(df[date_column], errors='coerce', utc=True)
    df = df.dropna(subset=[date_column])

    # Parse reference_date as timezone-aware UTC as well so both sides match
    # If reference_date is already a datetime with tzinfo, to_datetime(..., utc=True)
    # will convert it to UTC-aware; if it's naive, it will be assumed as UTC.
    reference_date = pd.to_datetime(reference_date, errors='coerce', utc=True)

    pre_df = df[df[date_column] < reference_date]
    post_df = df[df[date_column] >= reference_date]

    return pre_df, post_df

def detect_date_col(df, candidates):
    """Return the first candidate column that exists in df or None."""
    for c in candidates:
        if c in df.columns:
            return c
    return None

def compute_yearly_volumes(prepared_map, reference_date):
    """
    prepared_map: dict mapping name -> (df, date_col, contributor_col)
    reference_date: string or datetime to split pre/post for common contributor detection

    Returns dict: {name: { total_per_year: {...}, full_per_year: {...}, common_per_year: {...} }}
    """
    volumes = {}
    for name, (df, date_col, contributor_col) in prepared_map.items():
        if df is None or df.empty or date_col is None:
            volumes[name] = {
                'total_per_year': {},
                'full_per_year': {},
                'common_per_year': {}
            }
            continue

        tmp = df.copy()
        tmp[date_col] = pd.to_datetime(tmp[date_col], errors='coerce', utc=True)
        tmp = tmp.dropna(subset=[date_col])
        if tmp.empty:
            volumes[name] = {
                'total_per_year': {},
                'full_per_year': {},
                'common_per_year': {}
            }
            continue

        tmp['year'] = tmp[date_col].dt.year.astype(str)
        total_per_year = tmp.groupby('year').size().to_dict()
        full_per_year = {k: int(v) for k, v in total_per_year.items()}

        common_per_year = {}
        if contributor_col and contributor_col in tmp.columns:
            # Identify common contributors between pre/post (wrt reference_date)
            pre_df, post_df = split_by_reference_date(tmp, date_col, reference_date)
            pre_clean = pre_df[pre_df[contributor_col] != 'P n/a'] if not pre_df.empty else pd.DataFrame()
            post_clean = post_df[post_df[contributor_col] != 'P n/a'] if not post_df.empty else pd.DataFrame()
            common = set(pre_clean[contributor_col].unique()) & set(post_clean[contributor_col].unique())
            if len(common) > 0:
                common_df = tmp[tmp[contributor_col].isin(common)]
                common_per_year = common_df.groupby('year').size().to_dict()
            else:
                common_per_year = {}

        volumes[name] = {
            'total_per_year': total_per_year,
            'full_per_year': full_per_year,
            'common_per_year': common_per_year
        }
    return volumes

def analyze_description_patterns(json_path, reference_date):
    """
    Run Mann-Whitney tests on description pattern JSON files - ONLY for common contributors.

    The JSON is expected to have a top-level key 'descriptionPatterns' with sub-sections
    like 'mrs_analysis' and 'commits_analysis'. The commits_analysis section contains a 'byYear'
    structure where each year has a 'patterns' list with items containing: 'pattern', 'count', 
    'contributors', and 'latestDate'.

    For each pattern we analyze only common contributors between pre/post periods.
    """
    results = {}
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        return { 'error': f'Could not load JSON: {e}' }

    sections = data.get('descriptionPatterns', {})
    
    # Parse reference date to determine pre/post years
    ref_date = pd.to_datetime(reference_date, utc=True)
    ref_year = ref_date.year
    
    for section_key, section_val in sections.items():
        if not isinstance(section_val, dict):
            results[section_key] = {}
            continue
            
        # Check if this section uses the byYear structure
        if 'byYear' in section_val:
            by_year_data = section_val['byYear']
            section_result = {}
            
            # Get all patterns across all years
            all_patterns = set()
            for year_str, year_data in by_year_data.items():
                if isinstance(year_data, dict) and 'patterns' in year_data:
                    for item in year_data['patterns']:
                        all_patterns.add(item.get('pattern'))
            
            # For each pattern, analyze common contributors between pre and post years
            for pattern in all_patterns:
                if not pattern:
                    continue
                    
                # Collect data for pre period (years < ref_year)
                pre_contribs = set()
                contrib_pre_counts = {}
                total_pre_commits = 0
                
                # Collect data for post period (years >= ref_year)
                post_contribs = set()
                contrib_post_counts = {}
                total_post_commits = 0
                
                for year_str, year_data in by_year_data.items():
                    try:
                        year = int(year_str)
                    except (ValueError, TypeError):
                        continue
                        
                    if not isinstance(year_data, dict) or 'patterns' not in year_data:
                        continue
                    
                    # Find this pattern in the year's patterns
                    for item in year_data['patterns']:
                        if item.get('pattern') != pattern:
                            continue
                            
                        count = item.get('count', 0)
                        contributors = item.get('contributors', [])
                        
                        if isinstance(contributors, str):
                            try:
                                contributors = json.loads(contributors)
                            except Exception:
                                contributors = []
                        
                        # Assign to pre or post based on year
                        if year < ref_year:
                            total_pre_commits += count
                            for c in contributors:
                                pre_contribs.add(c)
                                contrib_pre_counts[c] = contrib_pre_counts.get(c, 0) + 1
                        else:
                            total_post_commits += count
                            for c in contributors:
                                post_contribs.add(c)
                                contrib_post_counts[c] = contrib_post_counts.get(c, 0) + 1
                
                # Analyze ONLY common contributors
                common = sorted(pre_contribs & post_contribs)
                metric_name = f"{section_key}:{pattern}"
                patt_result = {}
                
                if len(common) > 0:
                    pre_counts = [contrib_pre_counts.get(c, 0) for c in common]
                    post_counts = [contrib_post_counts.get(c, 0) for c in common]
                    
                    # Run Mann-Whitney test for common contributors
                    if any(pre_counts) or any(post_counts):
                        patt_result['contributors_common'] = perform_mann_whitney(
                            pre_counts, post_counts, 
                            f"{metric_name} - common contributors", 
                            common_contributors=list(common)
                        )
                        
                        # Add volume statistics
                        patt_result['contributors_common']['totalCommits_n1'] = total_pre_commits
                        patt_result['contributors_common']['totalCommits_n2'] = total_post_commits
                        patt_result['contributors_common']['uniquePersons_n1'] = len(pre_contribs)
                        patt_result['contributors_common']['uniquePersons_n2'] = len(post_contribs)
                        patt_result['contributors_common']['commonPersonsCount'] = len(common)
                    else:
                        # Fallback to binary 1s if counts are all zeros
                        patt_result['contributors_common'] = perform_mann_whitney(
                            [1]*len(common), [1]*len(common), 
                            f"{metric_name} - common contributors", 
                            common_contributors=list(common)
                        )
                        patt_result['contributors_common']['totalCommits_n1'] = total_pre_commits
                        patt_result['contributors_common']['totalCommits_n2'] = total_post_commits
                        patt_result['contributors_common']['uniquePersons_n1'] = len(pre_contribs)
                        patt_result['contributors_common']['uniquePersons_n2'] = len(post_contribs)
                        patt_result['contributors_common']['commonPersonsCount'] = len(common)
                else:
                    # No common contributors found
                    patt_result['contributors_common'] = {
                        'metric': f"{metric_name} - common contributors",
                        'error': 'No common contributors between pre and post',
                        'totalCommits_n1': total_pre_commits,
                        'totalCommits_n2': total_post_commits,
                        'uniquePersons_n1': len(pre_contribs),
                        'uniquePersons_n2': len(post_contribs),
                        'commonPersonsCount': 0,
                        'n1': int(len(pre_contribs)),
                        'n2': int(len(post_contribs))
                    }
                
                section_result[pattern] = patt_result
            
            results[section_key] = section_result
        
        else:
            # Old format: patterns list with date field
            section_patterns = section_val.get('patterns', []) if isinstance(section_val, dict) else []
            if not section_patterns:
                results[section_key] = {}
                continue

            # Build DataFrame of pattern, count, date, contributors
            rows = []
            for item in section_patterns:
                patt = item.get('pattern')
                cnt = item.get('count')
                date = item.get('date')
                contributors = item.get('contributors', [])
                
                # Skip items without contributors
                if contributors is None or contributors == []:
                   continue
                    
                if patt is None or cnt is None or date is None:
                    continue
                rows.append({'pattern': patt, 'count': cnt, 'date': date, 'contributors': contributors})

            if not rows:
                results[section_key] = {}
                continue

            df = pd.DataFrame(rows)
            df['date'] = pd.to_datetime(df['date'], errors='coerce', utc=True)
            df = df.dropna(subset=['date'])

            # Split by reference date
            pre_df, post_df = split_by_reference_date(df, 'date', reference_date)

            section_result = {}
            
            # For each pattern, analyze ONLY common contributors
            for patt in df['pattern'].unique():
                # Get rows for this pattern from pre and post periods (already split)
                patt_rows_pre = pre_df[pre_df['pattern'] == patt]
                patt_rows_post = post_df[post_df['pattern'] == patt]

                # Build sets of contributors present in pre and post (based on entry dates)
                pre_contribs = set()
                post_contribs = set()
                contrib_pre_counts = {}
                contrib_post_counts = {}
                
                # Also track total commits per period for this pattern
                total_pre_commits = 0
                total_post_commits = 0

                # Process PRE period rows
                for _, row in patt_rows_pre.iterrows():
                    row_count = row.get('count', 0)
                    contributors = row.get('contributors') or []
                    
                    if isinstance(contributors, str):
                        try:
                            contributors = json.loads(contributors)
                        except Exception:
                            contributors = []
                    
                    total_pre_commits += row_count
                    for c in contributors:
                        pre_contribs.add(c)
                        contrib_pre_counts[c] = contrib_pre_counts.get(c, 0) + 1
                
                # Process POST period rows
                for _, row in patt_rows_post.iterrows():
                    row_count = row.get('count', 0)
                    contributors = row.get('contributors') or []
                    
                    if isinstance(contributors, str):
                        try:
                            contributors = json.loads(contributors)
                        except Exception:
                            contributors = []
                    
                    total_post_commits += row_count
                    for c in contributors:
                        post_contribs.add(c)
                        contrib_post_counts[c] = contrib_post_counts.get(c, 0) + 1

                # ONLY analyze common contributors
                common = sorted(pre_contribs & post_contribs)
                metric_name = f"{section_key}:{patt}"
                patt_result = {}
                
                if len(common) > 0:
                    pre_counts = [contrib_pre_counts.get(c, 0) for c in common]
                    post_counts = [contrib_post_counts.get(c, 0) for c in common]
                    
                    # Run Mann-Whitney test for common contributors
                    if any(pre_counts) or any(post_counts):
                        patt_result['contributors_common'] = perform_mann_whitney(
                            pre_counts, post_counts, 
                            f"{metric_name} - common contributors", 
                            common_contributors=list(common)
                        )
                        
                        # Add volume statistics
                        patt_result['contributors_common']['totalCommits_n1'] = total_pre_commits
                        patt_result['contributors_common']['totalCommits_n2'] = total_post_commits
                        patt_result['contributors_common']['uniquePersons_n1'] = len(pre_contribs)
                        patt_result['contributors_common']['uniquePersons_n2'] = len(post_contribs)
                        patt_result['contributors_common']['commonPersonsCount'] = len(common)
                    else:
                        # Fallback to binary 1s if counts are all zeros
                        patt_result['contributors_common'] = perform_mann_whitney(
                            [1]*len(common), [1]*len(common), 
                            f"{metric_name} - common contributors", 
                            common_contributors=list(common)
                        )
                        patt_result['contributors_common']['totalCommits_n1'] = total_pre_commits
                        patt_result['contributors_common']['totalCommits_n2'] = total_post_commits
                        patt_result['contributors_common']['uniquePersons_n1'] = len(pre_contribs)
                        patt_result['contributors_common']['uniquePersons_n2'] = len(post_contribs)
                        patt_result['contributors_common']['commonPersonsCount'] = len(common)
                else:
                    # No common contributors found
                    patt_result['contributors_common'] = {
                        'metric': f"{metric_name} - common contributors",
                        'error': 'No common contributors between pre and post',
                        'totalCommits_n1': total_pre_commits,
                        'totalCommits_n2': total_post_commits,
                        'uniquePersons_n1': len(pre_contribs),
                        'uniquePersons_n2': len(post_contribs),
                        'commonPersonsCount': 0,
                        'n1': int(len(pre_contribs)),
                        'n2': int(len(post_contribs))
                    }

                section_result[patt] = patt_result

            results[section_key] = section_result

    return results

# ============================================================================
# RQ1: FEEDBACK LOOPS
# ============================================================================

def analyze_rq1_feedback_loops(commits_df, mrs_df, pipelines_df, reference_date, workforce_mode='both'):
    """
    Analyze RQ1: Feedback Loops
    Metrics:
    - Pipeline execution frequency
    - Pipeline success/failure rates
    - Build duration and trends
    - Deployment frequency
    - MR/PR creation rate
    - MR/PR review time
    - MR/PR merge time
    - Code review participation
    """
    print("\n" + "="*60)
    print("RQ1: FEEDBACK LOOPS ANALYSIS")
    print("="*60)
    
    results = {}
    
    # Choose the Created date key depending on available columns in pipelines_df.
    # Prefer 'created_on' (Bitbucket) if present, otherwise fall back to 'created_at'.
    Created_key = 'created_at'
    if not pipelines_df.empty:
        if 'created_on' in pipelines_df.columns:
            Created_key = 'created_on'
        elif 'created_at' in pipelines_df.columns:
            Created_key = 'created_at'
    # ---- Pipeline Metrics ----
    if not pipelines_df.empty and Created_key in pipelines_df.columns:
        print("\n📊 Analyzing Pipeline Metrics...")
        
        updated_key = 'updated_at'
        if 'completed_on' in pipelines_df.columns:
            updated_key = 'completed_on'
            
        # Ensure the Created and updated timestamp columns are parsed as timezone-aware datetimes
        # to avoid subtraction of string objects which raises TypeError.
        pipelines_df[Created_key] = pd.to_datetime(pipelines_df[Created_key], errors='coerce', utc=True)
        pipelines_df[updated_key] = pd.to_datetime(pipelines_df[updated_key], errors='coerce', utc=True)
        pipelines_df['duration_minutes'] = (pipelines_df[updated_key] - pipelines_df[Created_key]).dt.total_seconds() / 60

        pre_pipelines, post_pipelines = split_by_reference_date(
            pipelines_df, Created_key, reference_date
        )
        

        pre_duration = pre_pipelines['duration_minutes'].dropna()
        pre_duration = pre_duration[pre_duration > 0].values

        post_duration = post_pipelines['duration_minutes'].dropna()
        post_duration = post_duration[post_duration > 0].values

        if len(pre_duration) > 0 and len(post_duration) > 0:
            results['buildDuration'] = perform_mann_whitney(
                pre_duration, post_duration, "Build Duration (minutes)"
            )
            print(f"   ✓ Build Duration: p={results['buildDuration']['pValue']:.4f}")

        # detect contributor column for pipeline records (if present)
        pipeline_contrib_col = None
        for c in ['anonymized_name', 'author', 'user', 'triggered_by', 'creator', 'author_name']:
            if c in pipelines_df.columns:
                pipeline_contrib_col = c
                break

        # Build contributor sets for pipeline pre/post
        pre_pipeline_contribs = set()
        post_pipeline_contribs = set()
        common_pipeline_contribs = set()
        if pipeline_contrib_col:
            pre_pipeline_clean = pre_pipelines[pre_pipelines[pipeline_contrib_col] != 'P n/a'] if not pre_pipelines.empty else pd.DataFrame()
            post_pipeline_clean = post_pipelines[post_pipelines[pipeline_contrib_col] != 'P n/a'] if not post_pipelines.empty else pd.DataFrame()
            pre_pipeline_contribs = set(pre_pipeline_clean[pipeline_contrib_col].unique())
            post_pipeline_contribs = set(post_pipeline_clean[pipeline_contrib_col].unique())
            common_pipeline_contribs = pre_pipeline_contribs & post_pipeline_contribs

        # Pipeline Execution Frequency (per week) - full and common
        try:
            pre_pipelines['week'] = pre_pipelines[Created_key].dt.to_period('W')
            post_pipelines['week'] = post_pipelines[Created_key].dt.to_period('W')

            # full workforce
            pre_freq = pre_pipelines.groupby('week').size().values
            post_freq = post_pipelines.groupby('week').size().values
            if len(pre_freq) > 0 and len(post_freq) > 0:
                results['pipelineExecutionFrequency_full'] = perform_mann_whitney(
                    pre_freq, post_freq, 'Pipeline Execution Frequency (per week) - Full Workforce',
                    all_contributors_pre=list(pre_pipeline_contribs), all_contributors_post=list(post_pipeline_contribs)
                )
                print(f"   ✓ Pipeline Execution Freq (Full): p={results['pipelineExecutionFrequency_full']['pValue']:.4f}")

            # common contributors
            if pipeline_contrib_col and len(common_pipeline_contribs) > 0:
                pre_p_common = pre_pipelines[pre_pipelines[pipeline_contrib_col].isin(common_pipeline_contribs)]
                post_p_common = post_pipelines[post_pipelines[pipeline_contrib_col].isin(common_pipeline_contribs)]
                pre_freq_c = pre_p_common.groupby(pre_p_common[Created_key].dt.to_period('W')).size().values
                post_freq_c = post_p_common.groupby(post_p_common[Created_key].dt.to_period('W')).size().values
                if len(pre_freq_c) > 0 and len(post_freq_c) > 0:
                    results['pipelineExecutionFrequency_common'] = perform_mann_whitney(
                        pre_freq_c, post_freq_c, 'Pipeline Execution Frequency (per week) - Common Contributors',
                        common_contributors=list(common_pipeline_contribs)
                    )
                    print(f"   ✓ Pipeline Execution Freq (Common): p={results['pipelineExecutionFrequency_common']['pValue']:.4f}")
        except Exception:
            pass

        # Pipeline Success Rate (per week) - detect success column and compute weekly success rate
        try:
            # detect status-like column
            status_col = None
            for c in ['status', 'state', 'result', 'outcome', 'result_name']:
                if c in pipelines_df.columns:
                    status_col = c
                    break
            if status_col is not None:
                def is_success(v):
                    if pd.isna(v):
                        return 0
                    if isinstance(v, (int, float)):
                        return 1 if v > 0 else 0
                    s = str(v).strip().lower()
                    return 1 if s in ('success', 'passed', 'succeeded', 'successful', 'ok', 'completed') else 0

                pipelines_df['success_flag'] = pipelines_df[status_col].apply(is_success)
                # recompute pre/post using flagged df
                pre_p = pipelines_df[pipelines_df[Created_key] < pd.to_datetime(reference_date, utc=True)]
                post_p = pipelines_df[pipelines_df[Created_key] >= pd.to_datetime(reference_date, utc=True)]
                if not pre_p.empty and not post_p.empty:
                    pre_rate = (pre_p.groupby(pre_p[Created_key].dt.to_period('W'))['success_flag'].mean() * 100).values
                    post_rate = (post_p.groupby(post_p[Created_key].dt.to_period('W'))['success_flag'].mean() * 100).values
                    if len(pre_rate) > 0 and len(post_rate) > 0:
                        results['pipelineSuccessRate_full'] = perform_mann_whitney(
                            pre_rate, post_rate, 'Pipeline Success Rate (weekly % ) - Full Workforce'
                        )
                        print(f"   ✓ Pipeline Success Rate (Full): p={results['pipelineSuccessRate_full']['pValue']:.4f}")

                    # common contributors success rate
                    if pipeline_contrib_col and len(common_pipeline_contribs) > 0:
                        pre_p_c = pre_p[pre_p[pipeline_contrib_col].isin(common_pipeline_contribs)]
                        post_p_c = post_p[post_p[pipeline_contrib_col].isin(common_pipeline_contribs)]
                        if not pre_p_c.empty and not post_p_c.empty:
                            pre_rate_c = (pre_p_c.groupby(pre_p_c[Created_key].dt.to_period('W'))['success_flag'].mean() * 100).values
                            post_rate_c = (post_p_c.groupby(post_p_c[Created_key].dt.to_period('W'))['success_flag'].mean() * 100).values
                            if len(pre_rate_c) > 0 and len(post_rate_c) > 0:
                                results['pipelineSuccessRate_common'] = perform_mann_whitney(
                                    pre_rate_c, post_rate_c, 'Pipeline Success Rate (weekly % ) - Common Contributors',
                                    common_contributors=list(common_pipeline_contribs)
                                )
                                print(f"   ✓ Pipeline Success Rate (Common): p={results['pipelineSuccessRate_common']['pValue']:.4f}")
        except Exception:
            pass
    
    # ---- MR/PR Metrics ----
    if not mrs_df.empty and Created_key in mrs_df.columns:
        print("\n📊 Analyzing MR/PR Metrics...")
        
        pre_mrs, post_mrs = split_by_reference_date(
            mrs_df, Created_key, reference_date
        )
        
        # Get contributors for workforce analysis
        pre_contributors = set()
        post_contributors = set()
        common_contributors = set()
        
        if 'anonymized_name' in mrs_df.columns:
            mrs_clean = mrs_df[mrs_df['anonymized_name'] != 'P n/a']
            pre_mrs_clean = pre_mrs[pre_mrs['anonymized_name'] != 'P n/a']
            post_mrs_clean = post_mrs[post_mrs['anonymized_name'] != 'P n/a']
            
            pre_contributors = set(pre_mrs_clean['anonymized_name'].unique())
            post_contributors = set(post_mrs_clean['anonymized_name'].unique())
            common_contributors = pre_contributors & post_contributors
        
        # 4. MR/PR Creation Rate (per week)
        if len(pre_mrs) > 0 and len(post_mrs) > 0:
            pre_mrs['week'] = pre_mrs[Created_key].dt.to_period('W')
            post_mrs['week'] = post_mrs[Created_key].dt.to_period('W')
            
            if workforce_mode in ['full', 'both']:
                pre_creation = pre_mrs.groupby('week').size().values
                post_creation = post_mrs.groupby('week').size().values
                
                results['mrCreationRate_full'] = perform_mann_whitney(
                    pre_creation, post_creation, "MR/PR Creation Rate (per week) - Full Workforce",
                    all_contributors_pre=list(pre_contributors),
                    all_contributors_post=list(post_contributors)
                )
                print(f"   ✓ MR Creation Rate (Full): p={results['mrCreationRate_full']['pValue']:.4f}")
            
            if workforce_mode in ['common', 'both'] and len(common_contributors) > 0:
                pre_mrs_common = pre_mrs[pre_mrs['anonymized_name'].isin(common_contributors)]
                post_mrs_common = post_mrs[post_mrs['anonymized_name'].isin(common_contributors)]
                
                pre_creation_common = pre_mrs_common.groupby('week').size().values
                post_creation_common = post_mrs_common.groupby('week').size().values
                
                if len(pre_creation_common) > 0 and len(post_creation_common) > 0:
                    results['mrCreationRate_common'] = perform_mann_whitney(
                        pre_creation_common, post_creation_common, 
                        "MR/PR Creation Rate (per week) - Common Contributors",
                        common_contributors=list(common_contributors)
                    )
                    print(f"   ✓ MR Creation Rate (Common): p={results['mrCreationRate_common']['pValue']:.4f}")
        
        duration_key = 'duration_hours'
        if not mrs_df.empty:
            if 'duration_hours' in mrs_df.columns:
                duration_key = 'duration_hours'
            if 'cycle_time_hours' in mrs_df.columns:
                duration_key = 'cycle_time_hours'
        # 5. MR/PR Review Time (hours)
        if duration_key in mrs_df.columns:
            pre_review_time = pd.to_numeric(pre_mrs[duration_key], errors='coerce')
            pre_review_time = pre_review_time[pre_review_time > 0].dropna().values
            
            post_review_time = pd.to_numeric(post_mrs[duration_key], errors='coerce')
            post_review_time = post_review_time[post_review_time > 0].dropna().values
            
            if len(pre_review_time) > 0 and len(post_review_time) > 0:
                # original aggregate result (kept for compatibility)
                results['mrReviewTime'] = perform_mann_whitney(
                    pre_review_time, post_review_time, "MR/PR Review Time (hours)",
                    all_contributors_pre=list(pre_contributors), all_contributors_post=list(post_contributors)
                )
                print(f"   ✓ MR Review Time: p={results['mrReviewTime']['pValue']:.4f}")

                # Full workforce variant
                results['mrReviewTime_full'] = perform_mann_whitney(
                    pre_review_time, post_review_time, "MR/PR Review Time (hours) - Full Workforce",
                    all_contributors_pre=list(pre_contributors), all_contributors_post=list(post_contributors)
                )
                print(f"   ✓ MR Review Time (Full): p={results['mrReviewTime_full']['pValue']:.4f}")

                # Common contributors variant (filter by anonymized_name)
                if len(common_contributors) > 0:
                    pre_review_common = pre_mrs[pre_mrs['anonymized_name'].isin(common_contributors)][duration_key]
                    post_review_common = post_mrs[post_mrs['anonymized_name'].isin(common_contributors)][duration_key]
                    pre_rev_common_vals = pd.to_numeric(pre_review_common, errors='coerce').dropna().values
                    post_rev_common_vals = pd.to_numeric(post_review_common, errors='coerce').dropna().values
                    if len(pre_rev_common_vals) > 0 and len(post_rev_common_vals) > 0:
                        results['mrReviewTime_common'] = perform_mann_whitney(
                            pre_rev_common_vals, post_rev_common_vals, "MR/PR Review Time (hours) - Common Contributors",
                            common_contributors=list(common_contributors)
                        )
                        print(f"   ✓ MR Review Time (Common): p={results['mrReviewTime_common']['pValue']:.4f}")

        state_key = 'state'
        if not mrs_df.empty:
            if 'state' in mrs_df.columns:
                state_key = 'state'
            if 'pr_state' in mrs_df.columns:
                state_key = 'pr_state'

        # 6. MR/PR Merge Time (hours) - only for merged MRs
        if state_key in mrs_df.columns and duration_key in mrs_df.columns:
            pre_mrs[state_key] = pre_mrs[state_key].astype('string').str.lower()
            post_mrs[state_key] = post_mrs[state_key].astype('string').str.lower()

            pre_merged = pre_mrs[pre_mrs[state_key] == 'merged']
            post_merged = post_mrs[post_mrs[state_key] == 'merged']
            
            pre_merge_time = pd.to_numeric(pre_merged[duration_key], errors='coerce')
            pre_merge_time = pre_merge_time[pre_merge_time > 0].dropna().values
            
            post_merge_time = pd.to_numeric(post_merged[duration_key], errors='coerce')
            post_merge_time = post_merge_time[post_merge_time > 0].dropna().values
            
            if len(pre_merge_time) > 0 and len(post_merge_time) > 0:
                # original (aggregate) result kept for compatibility
                results['mrMergeTime'] = perform_mann_whitney(
                    pre_merge_time, post_merge_time, "MR/PR Merge Time (hours)",
                    all_contributors_pre=list(pre_contributors), all_contributors_post=list(post_contributors)
                )
                print(f"   ✓ MR Merge Time: p={results['mrMergeTime']['pValue']:.4f}")

                # Full workforce variant (explicit key)
                results['mrMergeTime_full'] = perform_mann_whitney(
                    pre_merge_time, post_merge_time, "MR/PR Merge Time (hours) - Full Workforce",
                    all_contributors_pre=list(pre_contributors), all_contributors_post=list(post_contributors)
                )
                print(f"   ✓ MR Merge Time (Full): p={results['mrMergeTime_full']['pValue']:.4f}")

                # Common contributors variant (filter merged MRs by common contributors)
                if len(common_contributors) > 0:
                    pre_merged_common = pre_merged[pre_merged['anonymized_name'].isin(common_contributors)]
                    post_merged_common = post_merged[post_merged['anonymized_name'].isin(common_contributors)]
                    pre_merge_common = pd.to_numeric(pre_merged_common[duration_key], errors='coerce').dropna().values
                    post_merge_common = pd.to_numeric(post_merged_common[duration_key], errors='coerce').dropna().values
                    if len(pre_merge_common) > 0 and len(post_merge_common) > 0:
                        results['mrMergeTime_common'] = perform_mann_whitney(
                            pre_merge_common, post_merge_common, "MR/PR Merge Time (hours) - Common Contributors",
                            common_contributors=list(common_contributors)
                        )
                        print(f"   ✓ MR Merge Time (Common): p={results['mrMergeTime_common']['pValue']:.4f}")
        
        # 7. Code Review Participation (reviewers per MR)
        if 'reviewers_count' in mrs_df.columns:
            pre_reviewers = pd.to_numeric(pre_mrs['reviewers_count'], errors='coerce').dropna().values
            post_reviewers = pd.to_numeric(post_mrs['reviewers_count'], errors='coerce').dropna().values
            
            if len(pre_reviewers) > 0 and len(post_reviewers) > 0:
                # keep original
                results['codeReviewParticipation'] = perform_mann_whitney(
                    pre_reviewers, post_reviewers, "Code Review Participation (reviewers per MR)",
                    all_contributors_pre=list(pre_contributors), all_contributors_post=list(post_contributors)
                )
                print(f"   ✓ Code Review Participation: p={results['codeReviewParticipation']['pValue']:.4f}")

                # Full workforce variant
                results['codeReviewParticipation_full'] = perform_mann_whitney(
                    pre_reviewers, post_reviewers, "Code Review Participation (reviewers per MR) - Full Workforce",
                    all_contributors_pre=list(pre_contributors), all_contributors_post=list(post_contributors)
                )
                print(f"   ✓ Code Review Participation (Full): p={results['codeReviewParticipation_full']['pValue']:.4f}")

                # Common contributors variant (filter by author)
                if len(common_contributors) > 0:
                    pre_rev_common = pre_mrs[pre_mrs['anonymized_name'].isin(common_contributors)][ 'reviewers_count']
                    post_rev_common = post_mrs[post_mrs['anonymized_name'].isin(common_contributors)]['reviewers_count']
                    pre_rev_common_vals = pd.to_numeric(pre_rev_common, errors='coerce').dropna().values
                    post_rev_common_vals = pd.to_numeric(post_rev_common, errors='coerce').dropna().values
                    if len(pre_rev_common_vals) > 0 and len(post_rev_common_vals) > 0:
                        results['codeReviewParticipation_common'] = perform_mann_whitney(
                            pre_rev_common_vals, post_rev_common_vals, "Code Review Participation (reviewers per MR) - Common Contributors",
                            common_contributors=list(common_contributors)
                        )
                        print(f"   ✓ Code Review Participation (Common): p={results['codeReviewParticipation_common']['pValue']:.4f}")
    
    return results

# ============================================================================
# RQ2: COGNITIVE LOAD
# ============================================================================

def analyze_rq2_cognitive_load(commits_df, mrs_df, jira_df, churn_df, reference_date, workforce_mode='both'):
    """
    Analyze RQ2: Cognitive Load
    Metrics:
    - Commit frequency and patterns
    - Commit message structure
    - Code churn rates (commit-level)
    - Code churn rates (MR-level)
    - Issue cycle time
    - Context switching frequency
    - Operational ticket volume
    """
    print("\n" + "="*60)
    print("RQ2: COGNITIVE LOAD ANALYSIS")
    print("="*60)
    
    results = {}

    Created_key = 'created_at'
    if not commits_df.empty:
        if 'date' in commits_df.columns:
            Created_key = 'date'
        elif 'created_at' in commits_df.columns:
            Created_key = 'created_at'
    
    # ---- Commit Metrics ----
    if not commits_df.empty and Created_key in commits_df.columns:
        print("\n📊 Analyzing Commit Metrics...")
        
        if 'lines_added' in commits_df.columns and 'lines_deleted' in commits_df.columns:
            commits_df['commit_churn'] = (
                pd.to_numeric(commits_df['lines_added'], errors='coerce').fillna(0) +
                pd.to_numeric(commits_df['lines_deleted'], errors='coerce').fillna(0)
            )

        pre_commits, post_commits = split_by_reference_date(
            commits_df, Created_key, reference_date
        )
        
        # Get contributors
        pre_contributors = set()
        post_contributors = set()
        common_contributors = set()
        
        if 'anonymized_name' in commits_df.columns:
            commits_clean = commits_df[commits_df['anonymized_name'] != 'P n/a']
            pre_commits_clean = pre_commits[pre_commits['anonymized_name'] != 'P n/a']
            post_commits_clean = post_commits[post_commits['anonymized_name'] != 'P n/a']
            
            pre_contributors = set(pre_commits_clean['anonymized_name'].unique())
            post_contributors = set(post_commits_clean['anonymized_name'].unique())
            common_contributors = pre_contributors & post_contributors
        
        # 1. Commit Frequency (per week)
        if len(pre_commits) > 0 and len(post_commits) > 0:
            pre_commits['week'] = pre_commits[Created_key].dt.to_period('W')
            post_commits['week'] = post_commits[Created_key].dt.to_period('W')
            
            if workforce_mode in ['full', 'both']:
                pre_commit_freq = pre_commits.groupby('week').size().values
                post_commit_freq = post_commits.groupby('week').size().values
                
                results['commitFrequency_full'] = perform_mann_whitney(
                    pre_commit_freq, post_commit_freq, "Commit Frequency (per week) - Full Workforce",
                    all_contributors_pre=list(pre_contributors),
                    all_contributors_post=list(post_contributors)
                )
                print(f"   ✓ Commit Frequency (Full): p={results['commitFrequency_full']['pValue']:.4f}")
            
            if workforce_mode in ['common', 'both'] and len(common_contributors) > 0:
                pre_commits_common = pre_commits[pre_commits['anonymized_name'].isin(common_contributors)]
                post_commits_common = post_commits[post_commits['anonymized_name'].isin(common_contributors)]
                
                # Calculate total commit volume for common contributors
                pre_commit_volume = len(pre_commits_common)
                post_commit_volume = len(post_commits_common)
                
                pre_commit_freq_common = pre_commits_common.groupby('week').size().values
                post_commit_freq_common = post_commits_common.groupby('week').size().values
                
                if len(pre_commit_freq_common) > 0 and len(post_commit_freq_common) > 0:
                    results['commitFrequency_common'] = perform_mann_whitney(
                        pre_commit_freq_common, post_commit_freq_common,
                        "Commit Frequency (per week) - Common Contributors",
                        common_contributors=list(common_contributors)
                    )
                    # Add commit volume to results
                    results['commitFrequency_common']['commitVolume_n1'] = pre_commit_volume
                    results['commitFrequency_common']['commitVolume_n2'] = post_commit_volume
                    print(f"   ✓ Commit Frequency (Common): p={results['commitFrequency_common']['pValue']:.4f}, Volume n1={pre_commit_volume}, n2={post_commit_volume}")
        
        # 2. Code Churn (commit-level) - lines added + deleted
        if 'commit_churn' in commits_df.columns:
            
            pre_churn = pre_commits['commit_churn'].values
            post_churn = post_commits['commit_churn'].values
            
            if len(pre_churn) > 0 and len(post_churn) > 0:
                # original (aggregate) result kept for compatibility
                results['commitLevelChurn'] = perform_mann_whitney(
                    pre_churn, post_churn, "Code Churn (commit-level)"
                )
                print(f"   ✓ Commit-level Churn: p={results['commitLevelChurn']['pValue']:.4f}")

                # Full workforce variant
                results['commitLevelChurn_full'] = perform_mann_whitney(
                    pre_churn, post_churn, "Code Churn (commit-level) - Full Workforce",
                    all_contributors_pre=list(pre_contributors), all_contributors_post=list(post_contributors)
                )
                print(f"   ✓ Commit-level Churn (Full): p={results['commitLevelChurn_full']['pValue']:.4f}")

                # Common contributors variant
                if len(common_contributors) > 0:
                    pre_churn_common = pre_commits[pre_commits['anonymized_name'].isin(common_contributors)]['commit_churn'].values
                    post_churn_common = post_commits[post_commits['anonymized_name'].isin(common_contributors)]['commit_churn'].values
                    if len(pre_churn_common) > 0 and len(post_churn_common) > 0:
                        results['commitLevelChurn_common'] = perform_mann_whitney(
                            pre_churn_common, post_churn_common, "Code Churn (commit-level) - Common Contributors",
                            common_contributors=list(common_contributors)
                        )
                        print(f"   ✓ Commit-level Churn (Common): p={results['commitLevelChurn_common']['pValue']:.4f}")
        
        # If a precomputed commit_churn CSV was provided, use it as an alternative source
        if isinstance(churn_df, dict):
            cch = churn_df['commit_churn'].copy()
            #date_col_c = detect_date_col(cch, ['created_at', 'Created', 'date'])


            cch['date'] = pd.to_datetime(cch['year'].astype(str) + "-" + cch['month'].astype(str), format='%Y-%m', errors='coerce')
            date_col_c = 'date'
            #                year	month
            churn_col_c = None
            for c in ['total_churn', 'net_change', 'commits']:
                if c in cch.columns:
                    churn_col_c = c
                    break

            if date_col_c and churn_col_c:
                pre_cc, post_cc = split_by_reference_date(cch, date_col_c, reference_date)
                pre_vals = pd.to_numeric(pre_cc[churn_col_c], errors='coerce').dropna()
                # Filter out zero values to avoid median=0 from months with no activity
                pre_vals = pre_vals[pre_vals > 0].values
                post_vals = pd.to_numeric(post_cc[churn_col_c], errors='coerce').dropna()
                # Filter out zero values to avoid median=0 from months with no activity
                post_vals = post_vals[post_vals > 0].values


                if len(pre_vals) > 0 and len(post_vals) > 0:
                    results['commitLevelChurn_commit_churn_csv'] = perform_mann_whitney(
                        pre_vals, post_vals, "Code Churn (commit-level) - from commit_churn CSV",
                        common_contributors=list(common_contributors)  # Convert set to list
                    )
                    print(f"   ✓ Commit-level Churn (commit_churn CSV): p={results['commitLevelChurn_commit_churn_csv']['pValue']:.4f}")

        
        # 3. Commit Message Structure (average length)
        if 'message' in commits_df.columns:
            pre_msg_length = pre_commits['message'].dropna().str.len().values
            post_msg_length = post_commits['message'].dropna().str.len().values
            
            if len(pre_msg_length) > 0 and len(post_msg_length) > 0:
                results['commitMessageLength'] = perform_mann_whitney(
                    pre_msg_length, post_msg_length, "Commit Message Length (characters)"
                )
                print(f"   ✓ Commit Message Length: p={results['commitMessageLength']['pValue']:.4f}")
    
    Created_key_mr = 'created_at'
    if not mrs_df.empty:
        if 'created_on' in mrs_df.columns:
            Created_key_mr = 'created_on'
        elif 'created_at' in mrs_df.columns:
            Created_key_mr = 'created_at'
    # ---- MR-Level Churn ----
    if not mrs_df.empty and Created_key_mr in mrs_df.columns:
        print("\n📊 Analyzing MR-Level Churn...")
        # Calculate MR churn using formula
        if 'lines_added' in mrs_df.columns and 'lines_deleted' in mrs_df.columns and 'files_changed' in mrs_df.columns:
            mrs_df['mr_churn'] = (
                pd.to_numeric(mrs_df['lines_added'], errors='coerce').fillna(0) +
                pd.to_numeric(mrs_df['lines_deleted'], errors='coerce').fillna(0) +
                5.0 * pd.to_numeric(mrs_df['files_changed'], errors='coerce').fillna(0) +
                2.0 * np.sqrt(
                    pd.to_numeric(mrs_df['lines_added'], errors='coerce').fillna(0) +
                    pd.to_numeric(mrs_df['lines_deleted'], errors='coerce').fillna(0) +
                    pd.to_numeric(mrs_df['files_changed'], errors='coerce').fillna(0)
                )
            )
        
        pre_mrs, post_mrs = split_by_reference_date(
            mrs_df, Created_key_mr, reference_date
        )
        
        # 4. Code Churn (MR-level)
        if 'mr_churn' in mrs_df.columns:   
        
            pre_mr_churn = pre_mrs['mr_churn'].values
            post_mr_churn = post_mrs['mr_churn'].values
            
            if len(pre_mr_churn) > 0 and len(post_mr_churn) > 0:
                # original (aggregate) result kept for compatibility
                results['mrLevelChurn'] = perform_mann_whitney(
                    pre_mr_churn, post_mr_churn, "Code Churn (MR-level)"
                )
                print(f"   ✓ MR-level Churn: p={results['mrLevelChurn']['pValue']:.4f}")

                # Full workforce variant
                results['mrLevelChurn_full'] = perform_mann_whitney(
                    pre_mr_churn, post_mr_churn, "Code Churn (MR-level) - Full Workforce",
                    all_contributors_pre=list(pre_contributors), all_contributors_post=list(post_contributors)
                )
                print(f"   ✓ MR-level Churn (Full): p={results['mrLevelChurn_full']['pValue']:.4f}")

                # Common contributors variant
                if len(common_contributors) > 0:
                    pre_mr_churn_common = pre_mrs[pre_mrs['anonymized_name'].isin(common_contributors)]['mr_churn'].values
                    post_mr_churn_common = post_mrs[post_mrs['anonymized_name'].isin(common_contributors)]['mr_churn'].values
                    if len(pre_mr_churn_common) > 0 and len(post_mr_churn_common) > 0:
                        results['mrLevelChurn_common'] = perform_mann_whitney(
                            pre_mr_churn_common, post_mr_churn_common, "Code Churn (MR-level) - Common Contributors",
                            common_contributors=list(common_contributors)
                        )
                        print(f"   ✓ MR-level Churn (Common): p={results['mrLevelChurn_common']['pValue']:.4f}")
    
    # If separate precomputed PR churn CSV is provided, compute MR-level churn from it too
   
    if isinstance(churn_df, dict) and 'pr_churn' in churn_df and churn_df['pr_churn'] is not None and not churn_df['pr_churn'].empty:
        pr_churn_df = churn_df['pr_churn'].copy()
        # detect date column in pr_churn_df
        pr_churn_df['date'] = pd.to_datetime(pr_churn_df['year'].astype(str) + "-" + pr_churn_df['month'].astype(str), format='%Y-%m', errors='coerce')
        date_col = 'date'

        churn_col = None
        for c in ['mr_churn', 'pr_churn', 'churn', 'churn_value']:
            if c in pr_churn_df.columns:
                churn_col = c
                break
        if date_col and churn_col:
            pre_pr_churn, post_pr_churn = split_by_reference_date(pr_churn_df, date_col, reference_date)
            pre_vals = pd.to_numeric(pre_pr_churn[churn_col], errors='coerce').dropna().values
            post_vals = pd.to_numeric(post_pr_churn[churn_col], errors='coerce').dropna().values
            if len(pre_vals) > 0 and len(post_vals) > 0:
                results['mrLevelChurn_pr_churn_csv'] = perform_mann_whitney(
                    pre_vals, post_vals, "Code Churn (MR-level) - from pr_churn CSV"
                )
                print(f"   ✓ MR-level Churn (pr_churn CSV): p={results['mrLevelChurn_pr_churn_csv']['pValue']:.4f}")

    
    # ---- Jira Metrics ----

    if not jira_df.empty:
        print("\n📊 Analyzing Jira Metrics...")
        
        # 7. Tickets per Person per Week (Common Contributors)
        if 'Created' in jira_df.columns and 'anonymized_assignee' in jira_df.columns:
            print("\n   🔍 DEBUG: Starting Tickets per Person per Week analysis...")
            # Clean jira data to remove n/a assignees
            jira_clean = jira_df[jira_df['anonymized_assignee'].notna() & (jira_df['anonymized_assignee'] != 'Unassigned') & (jira_df['anonymized_assignee'] != '')]
            
            print(f"   🔍 DEBUG: Found {jira_clean.shape[0]} valid tickets with assignees.")
            if not jira_clean.empty:
                pre_jira_clean, post_jira_clean = split_by_reference_date(
                    jira_clean, 'Created', reference_date
                )
                
                # Get contributors
                pre_jira_contributors = set(pre_jira_clean['anonymized_assignee'].unique())
                post_jira_contributors = set(post_jira_clean['anonymized_assignee'].unique())
                common_jira_contributors = pre_jira_contributors & post_jira_contributors
                
                print(f"   🔍 DEBUG: Pre contributors: {len(pre_jira_contributors)}, Post contributors: {len(post_jira_contributors)}, Common: {len(common_jira_contributors)}")
                print(f"   🔍 DEBUG: Workforce mode: {workforce_mode}")
                
                if workforce_mode in ['full', 'both']:
                    print("   🔍 DEBUG: Calculating full workforce metrics...")
                    # Calculate tickets per person per week - full workforce
                    pre_jira_clean['week'] = pre_jira_clean['Created'].dt.to_period('W')
                    post_jira_clean['week'] = post_jira_clean['Created'].dt.to_period('W')
                    
                    pre_tickets_per_person = pre_jira_clean.groupby(['week', 'anonymized_assignee']).size()
                    post_tickets_per_person = post_jira_clean.groupby(['week', 'anonymized_assignee']).size()
                    
                    if len(pre_tickets_per_person) > 0 and len(post_tickets_per_person) > 0:
                        results['ticketsPerPersonPerWeek_full'] = perform_mann_whitney(
                            pre_tickets_per_person.values, post_tickets_per_person.values,
                            "Tickets per Person per Week - Full Workforce",
                            all_contributors_pre=list(pre_jira_contributors),
                            all_contributors_post=list(post_jira_contributors)
                        )
                        print(f"   ✓ Tickets/Person/Week (Full): p={results['ticketsPerPersonPerWeek_full']['pValue']:.4f}")
                
                if workforce_mode in ['common', 'both'] and len(common_jira_contributors) > 0:
                    print(f"   🔍 DEBUG: Calculating common contributors metrics for {len(common_jira_contributors)} people...")
                    # Calculate tickets per person per week - common contributors only
                    pre_jira_common = pre_jira_clean[pre_jira_clean['anonymized_assignee'].isin(common_jira_contributors)]
                    post_jira_common = post_jira_clean[post_jira_clean['anonymized_assignee'].isin(common_jira_contributors)]
                    
                    print(f"   🔍 DEBUG: Pre common tickets: {len(pre_jira_common)}, Post common tickets: {len(post_jira_common)}")
                    
                    pre_jira_common['week'] = pre_jira_common['Created'].dt.to_period('W')
                    post_jira_common['week'] = post_jira_common['Created'].dt.to_period('W')
                    
                    pre_tickets_per_person_common = pre_jira_common.groupby(['week', 'anonymized_assignee']).size()
                    post_tickets_per_person_common = post_jira_common.groupby(['week', 'anonymized_assignee']).size()
                    
                    print(f"   🔍 DEBUG: Pre per-person entries: {len(pre_tickets_per_person_common)}, Post per-person entries: {len(post_tickets_per_person_common)}")
                    
                    if len(pre_tickets_per_person_common) > 0 and len(post_tickets_per_person_common) > 0:
                        # Calculate total ticket volume for common contributors
                        pre_ticket_volume = len(pre_jira_common)
                        post_ticket_volume = len(post_jira_common)
                        
                        print(f"   🔍 DEBUG: Calling perform_mann_whitney for common contributors...")
                        results['ticketsPerPersonPerWeek_common'] = perform_mann_whitney(
                            pre_tickets_per_person_common.values, post_tickets_per_person_common.values,
                            "Tickets per Person per Week - Common Contributors",
                            common_contributors=list(common_jira_contributors)
                        )
                        # Add ticket volume to results
                        results['ticketsPerPersonPerWeek_common']['ticketVolume_n1'] = pre_ticket_volume
                        results['ticketsPerPersonPerWeek_common']['ticketVolume_n2'] = post_ticket_volume
                        print(f"   ✓ Tickets/Person/Week (Common): p={results['ticketsPerPersonPerWeek_common']['pValue']:.4f}, Volume n1={pre_ticket_volume}, n2={post_ticket_volume}")
                        print(f"   🔍 DEBUG: Successfully saved ticketsPerPersonPerWeek_common to results!")
                    else:
                        print(f"   ⚠️  DEBUG: Insufficient data for common contributors analysis")
                else:
                    print(f"   ⚠️  DEBUG: Skipping common contributors - mode={workforce_mode}, common count={len(common_jira_contributors)}")
            else:
                print("   ⚠️  DEBUG: No valid Jira tickets found after cleaning")
        else:
            print("   ⚠️  DEBUG: Missing 'Created' or 'anonymized_assignee' columns in Jira data")
        
        # 8. Tickets per Person per Month (Common Contributors)
        if 'Created' in jira_df.columns and 'anonymized_assignee' in jira_df.columns:
            print("\n   🔍 DEBUG: Starting Tickets per Person per Month analysis...")
            # Clean jira data to remove n/a assignees
            jira_clean = jira_df[jira_df['anonymized_assignee'].notna() & (jira_df['anonymized_assignee'] != 'Unassigned') & (jira_df['anonymized_assignee'] != '')]
            
            print(f"   🔍 DEBUG: Found {jira_clean.shape[0]} valid tickets with assignees (monthly).")
            if not jira_clean.empty:
                pre_jira_clean, post_jira_clean = split_by_reference_date(
                    jira_clean, 'Created', reference_date
                )
                
                # Get contributors
                pre_jira_contributors = set(pre_jira_clean['anonymized_assignee'].unique())
                post_jira_contributors = set(post_jira_clean['anonymized_assignee'].unique())
                common_jira_contributors = pre_jira_contributors & post_jira_contributors
                
                print(f"   🔍 DEBUG: Monthly - Pre contributors: {len(pre_jira_contributors)}, Post contributors: {len(post_jira_contributors)}, Common: {len(common_jira_contributors)}")
                
                if workforce_mode in ['full', 'both']:
                    print("   🔍 DEBUG: Calculating monthly full workforce metrics...")
                    # Calculate tickets per person per month - full workforce
                    pre_jira_clean['month'] = pre_jira_clean['Created'].dt.to_period('M')
                    post_jira_clean['month'] = post_jira_clean['Created'].dt.to_period('M')
                    
                    pre_tickets_per_person_monthly = pre_jira_clean.groupby(['month', 'anonymized_assignee']).size()
                    post_tickets_per_person_monthly = post_jira_clean.groupby(['month', 'anonymized_assignee']).size()
                    
                    if len(pre_tickets_per_person_monthly) > 0 and len(post_tickets_per_person_monthly) > 0:
                        results['ticketsPerPersonPerMonth_full'] = perform_mann_whitney(
                            pre_tickets_per_person_monthly.values, post_tickets_per_person_monthly.values,
                            "Tickets per Person per Month - Full Workforce",
                            all_contributors_pre=list(pre_jira_contributors),
                            all_contributors_post=list(post_jira_contributors)
                        )
                        print(f"   ✓ Tickets/Person/Month (Full): p={results['ticketsPerPersonPerMonth_full']['pValue']:.4f}")
                
                if workforce_mode in ['common', 'both'] and len(common_jira_contributors) > 0:
                    print(f"   🔍 DEBUG: Calculating monthly common contributors metrics for {len(common_jira_contributors)} people...")
                    # Calculate tickets per person per month - common contributors only
                    pre_jira_common = pre_jira_clean[pre_jira_clean['anonymized_assignee'].isin(common_jira_contributors)]
                    post_jira_common = post_jira_clean[post_jira_clean['anonymized_assignee'].isin(common_jira_contributors)]
                    
                    print(f"   🔍 DEBUG: Monthly - Pre common tickets: {len(pre_jira_common)}, Post common tickets: {len(post_jira_common)}")
                    
                    pre_jira_common['month'] = pre_jira_common['Created'].dt.to_period('M')
                    post_jira_common['month'] = post_jira_common['Created'].dt.to_period('M')
                    
                    pre_tickets_per_person_monthly_common = pre_jira_common.groupby(['month', 'anonymized_assignee']).size()
                    post_tickets_per_person_monthly_common = post_jira_common.groupby(['month', 'anonymized_assignee']).size()
                    
                    print(f"   🔍 DEBUG: Monthly - Pre per-person entries: {len(pre_tickets_per_person_monthly_common)}, Post per-person entries: {len(post_tickets_per_person_monthly_common)}")
                    
                    if len(pre_tickets_per_person_monthly_common) > 0 and len(post_tickets_per_person_monthly_common) > 0:
                        # Calculate total ticket volume for common contributors
                        pre_ticket_volume_monthly = len(pre_jira_common)
                        post_ticket_volume_monthly = len(post_jira_common)
                        
                        print(f"   🔍 DEBUG: Calling perform_mann_whitney for monthly common contributors...")
                        results['ticketsPerPersonPerMonth_common'] = perform_mann_whitney(
                            pre_tickets_per_person_monthly_common.values, post_tickets_per_person_monthly_common.values,
                            "Tickets per Person per Month - Common Contributors",
                            common_contributors=list(common_jira_contributors)
                        )
                        # Add ticket volume to results
                        results['ticketsPerPersonPerMonth_common']['ticketVolume_n1'] = pre_ticket_volume_monthly
                        results['ticketsPerPersonPerMonth_common']['ticketVolume_n2'] = post_ticket_volume_monthly
                        print(f"   ✓ Tickets/Person/Month (Common): p={results['ticketsPerPersonPerMonth_common']['pValue']:.4f}, Volume n1={pre_ticket_volume_monthly}, n2={post_ticket_volume_monthly}")
                        print(f"   🔍 DEBUG: Successfully saved ticketsPerPersonPerMonth_common to results!")
                    else:
                        print(f"   ⚠️  DEBUG: Insufficient data for monthly common contributors analysis")
                else:
                    print(f"   ⚠️  DEBUG: Skipping monthly common contributors - mode={workforce_mode}, common count={len(common_jira_contributors)}")
            else:
                print("   ⚠️  DEBUG: No valid Jira tickets found after cleaning (monthly)")
        else:
            print("   ⚠️  DEBUG: Missing 'Created' or 'anonymized_assignee' columns in Jira data (monthly)")
    
        # 5. Issue Cycle Time
        if 'Created' in jira_df.columns and 'Resolved' in jira_df.columns:
            # Parse timestamps as timezone-aware UTC before computing cycle time
            jira_df['Resolved'] = pd.to_datetime(jira_df['Resolved'], errors='coerce', utc=True)
            jira_df['Created'] = pd.to_datetime(jira_df['Created'], errors='coerce', utc=True)

            jira_df['cycle_time_hours'] = (
                jira_df['Resolved'] - jira_df['Created']
            ).dt.total_seconds() / 3600
            
            pre_jira, post_jira = split_by_reference_date(
                jira_df, 'Created', reference_date
            )
            
            pre_cycle = pre_jira['cycle_time_hours'].dropna()
            pre_cycle = pre_cycle[pre_cycle > 0].values
            
            post_cycle = post_jira['cycle_time_hours'].dropna()
            post_cycle = post_cycle[post_cycle > 0].values
            
            if len(pre_cycle) > 0 and len(post_cycle) > 0:
                results['issueCycleTime'] = perform_mann_whitney(
                    pre_cycle, post_cycle, "Issue Cycle Time (hours)"
                )
                print(f"   ✓ Issue Cycle Time: p={results['issueCycleTime']['pValue']:.4f}")
        
        # 6. Operational Ticket Volume (per week)
        if 'Created' in jira_df.columns:
            # Ensure Created is timezone-aware UTC
            jira_df['Created'] = pd.to_datetime(jira_df['Created'], errors='coerce', utc=True)
            jira_df['week'] = jira_df['Created'].dt.to_period('W')

            pre_jira, post_jira = split_by_reference_date(
                jira_df, 'Created', reference_date
            )
            
            pre_volume = pre_jira.groupby('week').size().values
            post_volume = post_jira.groupby('week').size().values
            
            if len(pre_volume) > 0 and len(post_volume) > 0:
                results['operationalTicketVolume'] = perform_mann_whitney(
                    pre_volume, post_volume, "Operational Ticket Volume (per week)"
                )
                print(f"   ✓ Ticket Volume: p={results['operationalTicketVolume']['pValue']:.4f}")
        

    ##Context switching frequency commit for each developer
    if not commits_df.empty and Created_key in commits_df.columns and 'anonymized_name' in commits_df.columns:
        print("\n📊 Analyzing Context Switching Frequency...")
        pre_commits, post_commits = split_by_reference_date(
            commits_df, Created_key, reference_date
        )
        # Get contributors
        pre_contributors = set(pre_commits['anonymized_name'].unique())
        post_contributors = set(post_commits['anonymized_name'].unique())
        common_contributors = pre_contributors & post_contributors
        # 7. Context Switching Frequency (active projects per developer)
        pre_switching = []
        post_switching = []
        if workforce_mode in ['full', 'both']:
            for person in pre_commits['anonymized_name'].unique():
                person_commits = pre_commits[pre_commits['anonymized_name'] == person]
                person_projects = person_commits['repository_slug'].unique()
                activeProjects = len(person_projects)
                pre_switching.append(activeProjects)
            for person in post_commits['anonymized_name'].unique():
                person_commits = post_commits[post_commits['anonymized_name'] == person]
                person_projects = person_commits['repository_slug'].unique()
                activeProjects = len(person_projects)
                post_switching.append(activeProjects)
            results['contextSwitching_full'] = perform_mann_whitney(
                pre_switching, post_switching, "Context Switching Frequency (active projects per developer) - Full Workforce",
                all_contributors_pre=list(pre_contributors),
                all_contributors_post=list(post_contributors)
            )
            print(f"   ✓ Context Switching Frequency (Full): p={results['contextSwitching_full']['pValue']:.4f}")
        if workforce_mode in ['common', 'both'] and len(common_contributors) > 0:
            for person in common_contributors:
                person_commits_pre = pre_commits[pre_commits['anonymized_name'] == person]
                person_projects_pre = person_commits_pre['repository_slug'].unique()
                activeProjects = len(person_projects_pre)
                pre_switching.append(activeProjects)
                person_commits_post = post_commits[post_commits['anonymized_name'] == person]
                person_projects_post = person_commits_post['repository_slug'].unique()
                activeProjects = len(person_projects_post)
                post_switching.append(activeProjects)
            results['contextSwitching_common'] = perform_mann_whitney(
                pre_switching, post_switching, "Context Switching Frequency (active projects per developer) - Common Contributors",
                common_contributors=list(common_contributors)
            )
            print(f"   ✓ Context Switching Frequency (Common): p={results['contextSwitching_common']['pValue']:.4f}")

    return results

# ============================================================================
# RQ3: FLOW STATE
# ============================================================================

def analyze_rq3_flow_state(commits_df, mrs_df, copilot_df, reference_date, workforce_mode='both'):
    """
    Analyze RQ3: Flow State
    Metrics:
    - Commits per developer
    - MRs per developer
    """
    print("\n" + "="*60)
    print("RQ3: FLOW STATE ANALYSIS")
    print("="*60)
    
    results = {}
    
    Created_key = 'created_at'
    if not commits_df.empty:
        if 'date' in commits_df.columns:
            Created_key = 'date'
        elif 'created_at' in commits_df.columns:
            Created_key = 'created_at'
    # ---- Developer Productivity Metrics ----
    if not commits_df.empty and Created_key in commits_df.columns and 'anonymized_name' in commits_df.columns:
        print("\n📊 Analyzing Developer Productivity...")
        
        commits_clean = commits_df[commits_df['anonymized_name'] != 'P n/a']
        pre_commits, post_commits = split_by_reference_date(
            commits_clean, Created_key, reference_date
        )
        
        # Get contributors
        pre_contributors = set(pre_commits['anonymized_name'].unique())
        post_contributors = set(post_commits['anonymized_name'].unique())
        common_contributors = pre_contributors & post_contributors
        
        # 1. Commits per Developer (per week)
        if len(pre_commits) > 0 and len(post_commits) > 0:
            pre_commits['week'] = pre_commits[Created_key].dt.to_period('W')
            post_commits['week'] = post_commits[Created_key].dt.to_period('W')
            
            if workforce_mode in ['full', 'both']:
                # Calculate commits per developer per week
                pre_commits_per_dev = pre_commits.groupby(['week', 'anonymized_name']).size()
                post_commits_per_dev = post_commits.groupby(['week', 'anonymized_name']).size()
                
                results['commitsPerDeveloper_full'] = perform_mann_whitney(
                    pre_commits_per_dev.values, post_commits_per_dev.values,
                    "Commits per Developer (per week) - Full Workforce",
                    all_contributors_pre=list(pre_contributors),
                    all_contributors_post=list(post_contributors)
                )
                print(f"   ✓ Commits/Dev (Full): p={results['commitsPerDeveloper_full']['pValue']:.4f}")
            
            if workforce_mode in ['common', 'both'] and len(common_contributors) > 0:
                pre_commits_common = pre_commits[pre_commits['anonymized_name'].isin(common_contributors)]
                post_commits_common = post_commits[post_commits['anonymized_name'].isin(common_contributors)]
                
                pre_commits_per_dev_common = pre_commits_common.groupby(['week', 'anonymized_name']).size()
                post_commits_per_dev_common = post_commits_common.groupby(['week', 'anonymized_name']).size()
                
                if len(pre_commits_per_dev_common) > 0 and len(post_commits_per_dev_common) > 0:
                    results['commitsPerDeveloper_common'] = perform_mann_whitney(
                        pre_commits_per_dev_common.values, post_commits_per_dev_common.values,
                        "Commits per Developer (per week) - Common Contributors",
                        common_contributors=list(common_contributors)
                    )
                    print(f"   ✓ Commits/Dev (Common): p={results['commitsPerDeveloper_common']['pValue']:.4f}")
    

    Created_key_mr = 'created_at'
    if not mrs_df.empty:
        if 'created_on' in mrs_df.columns:
            Created_key_mr = 'created_on'
        elif 'created_at' in mrs_df.columns:
            Created_key_mr = 'created_at'
    # 2. MRs per Developer
    if not mrs_df.empty and Created_key_mr in mrs_df.columns and 'anonymized_name' in mrs_df.columns:
        mrs_clean = mrs_df[mrs_df['anonymized_name'] != 'P n/a']
        pre_mrs, post_mrs = split_by_reference_date(
            mrs_clean, Created_key_mr, reference_date
        )
        
        # Get authors
        pre_authors = set(pre_mrs['anonymized_name'].unique())
        post_authors = set(post_mrs['anonymized_name'].unique())
        common_authors = pre_authors & post_authors
        
        if len(pre_mrs) > 0 and len(post_mrs) > 0:
            pre_mrs['week'] = pre_mrs[Created_key_mr].dt.to_period('W')
            post_mrs['week'] = post_mrs[Created_key_mr].dt.to_period('W')
            
            if workforce_mode in ['full', 'both']:
                pre_mrs_per_dev = pre_mrs.groupby(['week', 'anonymized_name']).size()
                post_mrs_per_dev = post_mrs.groupby(['week', 'anonymized_name']).size()
                
                results['mrsPerDeveloper_full'] = perform_mann_whitney(
                    pre_mrs_per_dev.values, post_mrs_per_dev.values,
                    "MRs per Developer (per week) - Full Workforce",
                    all_contributors_pre=list(pre_authors),
                    all_contributors_post=list(post_authors)
                )
                print(f"   ✓ MRs/Dev (Full): p={results['mrsPerDeveloper_full']['pValue']:.4f}")
            
            if workforce_mode in ['common', 'both'] and len(common_authors) > 0:
                pre_mrs_common = pre_mrs[pre_mrs['anonymized_name'].isin(common_authors)]
                post_mrs_common = post_mrs[post_mrs['anonymized_name'].isin(common_authors)]
                
                pre_mrs_per_dev_common = pre_mrs_common.groupby(['week', 'anonymized_name']).size()
                post_mrs_per_dev_common = post_mrs_common.groupby(['week', 'anonymized_name']).size()
                
                if len(pre_mrs_per_dev_common) > 0 and len(post_mrs_per_dev_common) > 0:
                    results['mrsPerDeveloper_common'] = perform_mann_whitney(
                        pre_mrs_per_dev_common.values, post_mrs_per_dev_common.values,
                        "MRs per Developer (per week) - Common Contributors",
                        common_contributors=list(common_authors)
                    )
                    print(f"   ✓ MRs/Dev (Common): p={results['mrsPerDeveloper_common']['pValue']:.4f}")
    

    return results

# ============================================================================
# MAIN PROCESSING
# ============================================================================

def load_csv_files(args):
    """Load CSV files based on arguments

    Returns commits_df, mrs_df, pipelines_df, jira_df, copilot_df, churn_dict
    churn_dict is a dict with optional keys 'commit_churn' and 'pr_churn' containing DataFrames
    """
    commits_df = pd.DataFrame()
    mrs_df = pd.DataFrame()
    pipelines_df = pd.DataFrame()
    jira_df = pd.DataFrame()
    copilot_df = pd.DataFrame()
    commit_churn_df = pd.DataFrame()
    pr_churn_df = pd.DataFrame()

    if args.commits_csv:
        print(f"📂 Loading commits from: {args.commits_csv}")
        commits_df = pd.read_csv(args.commits_csv)
        print(f"   ✓ {len(commits_df)} commits loaded")

    if args.mrs_csv:
        print(f"📂 Loading MRs from: {args.mrs_csv}")
        mrs_df = pd.read_csv(args.mrs_csv)
        print(f"   ✓ {len(mrs_df)} MRs loaded")

    if args.pipelines_csv:
        print(f"📂 Loading pipelines from: {args.pipelines_csv}")
        pipelines_df = pd.read_csv(args.pipelines_csv)
        print(f"   ✓ {len(pipelines_df)} pipelines loaded")

    if args.jira_csv:
        print(f"📂 Loading Jira data from: {args.jira_csv}")
        jira_df = pd.read_csv(args.jira_csv)
        print(f"   ✓ {len(jira_df)} issues loaded")

    if args.copilot_csv:
        print(f"📂 Loading Copilot metrics from: {args.copilot_csv}")
        copilot_df = pd.read_csv(args.copilot_csv)
        print(f"   ✓ {len(copilot_df)} records loaded")

    if args.churn_csv:
        print(f"📂 Loading churn data from: {args.churn_csv}")
        # try to read a generic churn CSV (may contain both commit and pr churn columns)
        generic = pd.read_csv(args.churn_csv)
        # heuristics: look for commit/pr churn columns
        if 'commit_churn' in generic.columns or 'commit_churn_value' in generic.columns:
            commit_churn_df = generic
            print(f"   ✓ {len(commit_churn_df)} commit churn records loaded from generic churn file")
        if 'pr_churn' in generic.columns or 'mr_churn' in generic.columns:
            pr_churn_df = generic
            print(f"   ✓ {len(pr_churn_df)} PR churn records loaded from generic churn file")

    # support explicit commit/pr churn CSV args or fallback to consolidated/churn_results defaults
    if args.commit_churn_csv:
        print(f"📂 Loading commit churn from: {args.commit_churn_csv}")
        commit_churn_df = pd.read_csv(args.commit_churn_csv)
        print(f"   ✓ {len(commit_churn_df)} commit churn records loaded")
    else:
        default_commit = 'consolidated/churn_results/commit_churn_bitbucket.csv'
        try:
            commit_churn_path = Path(default_commit)
            if commit_churn_path.exists() and commit_churn_df.empty:
                commit_churn_df = pd.read_csv(commit_churn_path)
                print(f"   ✓ Loaded default commit churn: {default_commit} ({len(commit_churn_df)} rows)")
        except Exception:
            pass

    if args.pr_churn_csv:
        print(f"📂 Loading PR churn from: {args.pr_churn_csv}")
        pr_churn_df = pd.read_csv(args.pr_churn_csv)
        print(f"   ✓ {len(pr_churn_df)} PR churn records loaded")
    else:
        default_pr = 'consolidated/churn_results/pr_churn_bitbucket.csv'
        try:
            pr_churn_path = Path(default_pr)
            if pr_churn_path.exists() and pr_churn_df.empty:
                pr_churn_df = pd.read_csv(pr_churn_path)
                print(f"   ✓ Loaded default PR churn: {default_pr} ({len(pr_churn_df)} rows)")
        except Exception:
            pass
    
    churn_dict = {'commit_churn': commit_churn_df, 'pr_churn': pr_churn_df}
    return commits_df, mrs_df, pipelines_df, jira_df, copilot_df, churn_dict

def convert_to_native_types(obj):
    """Recursively convert numpy/pandas types to native Python types"""
    if isinstance(obj, dict):
        return {key: convert_to_native_types(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_native_types(item) for item in obj]
    elif isinstance(obj, (np.integer, np.int64, np.int32, np.int16, np.int8)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64, np.float32, np.float16)):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return convert_to_native_types(obj.tolist())
    elif isinstance(obj, (np.bool_, bool)):
        return bool(obj)
    elif pd.isna(obj):
        return None
    else:
        return obj

def main():
    args = parse_args()
    
    print("="*70)
    print("📊 MANN-WHITNEY U TEST ANALYSIS FOR DEVEX METRICS")
    print("="*70)
    print(f"\n🗓️  Reference Date: {args.reference_date}")
    print(f"👥 Workforce Mode: {args.workforce_mode}")
    print(f"📤 Output File: {args.output}\n")
    
    # Load data
    commits_df = pd.DataFrame()
    mrs_df = pd.DataFrame()
    pipelines_df = pd.DataFrame()
    jira_df = pd.DataFrame()
    copilot_df = pd.DataFrame()
    churn_df = pd.DataFrame()
    
    if args.mode == 'csv':
        commits_df, mrs_df, pipelines_df, jira_df, copilot_df, churn_dict = load_csv_files(args)
    else:
        # TODO: Add JSON loading logic if needed
        print("⚠️  JSON mode not yet implemented. Use CSV mode with --mode csv")
        sys.exit(1)
    
    # Perform analyses
    results = {
        'metadata': {
            'referenceDate': args.reference_date,
            'workforceMode': args.workforce_mode,
            'analysisDate': datetime.now().isoformat(),
            'dataSourcesUsed': {
                'commits': not commits_df.empty,
                'mrs': not mrs_df.empty,
                'pipelines': not pipelines_df.empty,
                'jira': not jira_df.empty,
                'copilot': not copilot_df.empty
            }
        },
        'rq1_feedback_loops': analyze_rq1_feedback_loops(
            commits_df, mrs_df, pipelines_df, args.reference_date, args.workforce_mode
        ),
        'rq2_cognitive_load': analyze_rq2_cognitive_load(
            commits_df, mrs_df, jira_df, churn_dict, args.reference_date, args.workforce_mode
        ),
        'rq3_flow_state': analyze_rq3_flow_state(
            commits_df, mrs_df, copilot_df, args.reference_date, args.workforce_mode
        )
    }
    
    # Load and analyze description patterns
    description_patterns_results = {}
    for platform, path in [
        ('gitlab', 'consolidated/descriptionPatternGitlab.json'),
        ('bitbucket', 'consolidated/descriptionPatternBitbucket.json')
    ]:
        try:
            print(f"\n📂 Loading description patterns from: {path}")
            platform_results = analyze_description_patterns(path, args.reference_date)
            description_patterns_results[platform] = platform_results
            print(f"   ✓ {platform} description patterns analyzed")
        except Exception as e:
            print(f"⚠️  Failed to analyze {platform} description patterns: {e}")
    
    results['descriptionPatterns'] = description_patterns_results
    
    # Convert to native types for JSON serialization
    results = convert_to_native_types(results)
    
    # Save results
    print(f"\n💾 Saving results to: {args.output}")
    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    # Print summary
    print("\n" + "="*70)
    print("✅ ANALYSIS COMPLETE!")
    print("="*70)
    
    total_tests = (
        len(results['rq1_feedback_loops']) +
        len(results['rq2_cognitive_load']) +
        len(results['rq3_flow_state'])
    )
    
    print(f"\n📊 Summary:")
    print(f"   • RQ1 (Feedback Loops): {len(results['rq1_feedback_loops'])} metrics")
    print(f"   • RQ2 (Cognitive Load): {len(results['rq2_cognitive_load'])} metrics")
    print(f"   • RQ3 (Flow State): {len(results['rq3_flow_state'])} metrics")
    print(f"   • Total tests performed: {total_tests}")
    print(f"\n📄 Results saved to: {args.output}\n")

    if args.extract_table:
        extract_table_data(args.extract_table[0], args.extract_table[1], args.table_output)

if __name__ == '__main__':
    main()
