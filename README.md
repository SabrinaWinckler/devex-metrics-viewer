# Mann-Whitney U Test Analysis for DevEx Metrics

## Overview

This script performs Mann-Whitney U statistical tests to analyze the impact of interventions (e.g., AI tool adoption) on Developer Experience metrics across three research questions:

- **RQ1: Feedback Loops** - Pipeline and code review metrics
- **RQ2: Cognitive Load** - Commit patterns, code churn, and task management
- **RQ3: Flow State** - Developer productivity and AI tool usage

## Installation

Requires Python 3.7+ with the following packages:

```bash
pip install pandas numpy scipy
```

## Usage Examples

### Example 1: Analyze GitLab/Bitbucket Data

```bash
python mann_whitney_analysis.py \
  --commits-csv normalized/09-setembro/2024/gitlab_commits_20240901_to_20240930.csv \
  --mrs-csv normalized/09-setembro/2024/gitlab_mrs_20240901_to_20240930.csv \
  --pipelines-csv normalized/09-setembro/2024/gitlab_pipelines_20240901_to_20240930.csv \
  --reference-date 2024-09-15 \
  --mode csv \
  --output results_sept_2024.json
```

### Example 2: Full Analysis with All Data Sources

```bash
python mann_whitney_analysis.py \
  --commits-csv normalized/commits_consolidated.csv \
  --mrs-csv normalized/mrs_consolidated.csv \
  --pipelines-csv normalized/pipelines_consolidated.csv \
  --jira-csv ../jira-metrics/issues_export.csv \
  --copilot-csv ../metrics-viewer/copilot_metrics.csv \
  --reference-date 2024-10-08 \
  --mode csv \
  --workforce-mode both \
  --output rq_analysis_complete.json
```

### Example 3: Only Common Contributors

```bash
python mann_whitney_analysis.py \
  --commits-csv normalized/commits_all.csv \
  --mrs-csv normalized/mrs_all.csv \
  --reference-date 2024-10-08 \
  --mode csv \
  --workforce-mode common \
  --output analysis_common_only.json
```

## Input Data Requirements

### Required Columns by Data Source

#### Commits CSV (`--commits-csv`)
- `created_at` (datetime) - Commit timestamp
- `anonymized_name` (string) - Anonymized contributor name
- `lines_added` (int) - Lines added in commit
- `lines_deleted` (int) - Lines deleted in commit
- `message` (string) - Commit message

#### MRs CSV (`--mrs-csv`)
- `created_at` (datetime) - MR creation timestamp
- `anonymized_name` (string) - Anonymized author name
- `state` (string) - MR state (merged, open, closed)
- `duration_hours` (float) - Time from creation to merge/close
- `reviewers_count` (int) - Number of reviewers
- `lines_added` (int) - Lines added in MR
- `lines_deleted` (int) - Lines deleted in MR
- `files_changed` (int) - Files changed in MR

#### Pipelines CSV (`--pipelines-csv`)
- `created_at` (datetime) - Pipeline start timestamp
- `updated_at` (datetime) - Pipeline end timestamp
- `status` (string) - Pipeline status (success, failed)
- `ref` (string, optional) - Branch/tag name

#### Jira CSV (`--jira-csv`)
- `created` (datetime) - Issue creation date
- `resolved` (datetime) - Issue resolution date
- Issue type and priority fields (optional)

#### Copilot CSV (`--copilot-csv`)
- `date` or `created_at` (datetime) - Metric date
- `total_prompts` or `prompts` (int) - Total prompts sent
- `suggestions` or `suggestions_count` (int) - Code suggestions
- `acceptance_rate` or `acceptance_rate_count` (float) - Acceptance % by count
- `acceptance_rate_lines` (float) - Acceptance % by lines
- `active_users` or `engaged_users` (int) - Active users

## Output Format

The script generates a JSON file with the following structure:

```json
{
  "metadata": {
    "referenceDate": "2024-10-08",
    "workforceMode": "both",
    "analysisDate": "2025-10-23T...",
    "dataSourcesUsed": {
      "commits": true,
      "mrs": true,
      "pipelines": true,
      "jira": false,
      "copilot": false
    }
  },
  "rq1_feedback_loops": {
    "pipelineExecutionFrequency": {
      "metric": "Pipeline Execution Frequency (per week)",
      "statistic": 1234.5,
      "pValue": 0.0234,
      "significant": true,
      "effectSize": 0.45,
      "effectSizeInterpretation": "medium",
      "n1": 20,
      "n2": 22,
      "medianPre": 45.0,
      "medianPost": 67.0,
      "meanPre": 48.3,
      "meanPost": 69.1,
      "percentageChange": 48.89
    }
  },
  "rq2_cognitive_load": { ... },
  "rq3_flow_state": { ... }
}
```

## Metrics Mapped to Research Questions

### RQ1: Feedback Loops (Speed & Quality)
- ✅ Pipeline execution frequency
- ✅ Pipeline success rate (%)
- ✅ Build duration (minutes)
- ✅ MR/PR creation rate (per week)
- ✅ MR/PR review time (hours)
- ✅ MR/PR merge time (hours)
- ✅ Code review participation (reviewers per MR)

### RQ2: Cognitive Load (Effectiveness)
- ✅ Commit frequency (per week)
- ✅ Code churn - commit level (lines)
- ✅ Code churn - MR level (composite score)
- ✅ Commit message length (characters)
- ✅ Issue cycle time (hours)
- ✅ Operational ticket volume (per week)

### RQ3: Flow State (Impact & Utilization)
- ✅ Commits per developer (per week)
- ✅ MRs per developer (per week)
- ✅ Total prompts sent (AI tool)
- ✅ Code suggestions generated (AI tool)
- ✅ Acceptance rate by count (%)
- ✅ Acceptance rate by lines (%)
- ✅ AI tool engagement (active users)

## Workforce Analysis Modes

### Full Workforce (`--workforce-mode full`)
Compares **all developers** in pre vs post periods, even if different people.

**Use when:** You want to measure overall organizational impact.

### Common Contributors Only (`--workforce-mode common`)
Compares only developers who were **active in both periods**.

**Use when:** You want to control for workforce changes and measure individual-level impact.

### Both (`--workforce-mode both`, default)
Performs both analyses and includes results with suffixes `_full` and `_common`.

**Use when:** You want comprehensive analysis for comparison.

## Interpreting Results

### Effect Size (Cohen's r)
- **< 0.1**: Negligible effect
- **0.1 - 0.3**: Small effect
- **0.3 - 0.5**: Medium effect
- **> 0.5**: Large effect

### Statistical Significance
- **p < 0.05**: Statistically significant difference
- **p ≥ 0.05**: No statistically significant difference

### Practical Interpretation Example

```json
{
  "metric": "MR/PR Merge Time (hours)",
  "pValue": 0.0123,
  "significant": true,
  "effectSize": 0.52,
  "effectSizeInterpretation": "large",
  "medianPre": 24.5,
  "medianPost": 16.2,
  "percentageChange": -33.88
}
```

**Interpretation:** After the intervention, merge time decreased by 33.88% (from 24.5h to 16.2h). This is a statistically significant change (p=0.012) with a large effect size (r=0.52), indicating a meaningful improvement in the feedback loop speed.

## Code Churn Formula

The MR-level code churn uses a composite formula:

```
MR_Churn = α × (lines_added + lines_deleted) + β × files_changed + γ × √(total_changes)
```

Where:
- α = 1.0 (weight for line changes)
- β = 5.0 (weight for file changes - higher impact)
- γ = 2.0 (weight for complexity normalization)

This formula accounts for both volume and complexity of changes.

## Tips for Your Study

1. **Choose Reference Date Carefully**: Pick the date when the intervention (e.g., AI tool) was introduced.

2. **Consolidate Data First**: If analyzing multiple months, consolidate your CSVs before running the analysis.

3. **Check Data Quality**: Ensure date columns are properly formatted and numeric columns don't have missing values.

4. **Run Both Modes**: Use `--workforce-mode both` to see how results differ between full workforce and common contributors.

5. **Export for Paper**: The JSON output can be easily converted to tables for your manuscript.

## Troubleshooting

### "Insufficient data" errors
- Check that your reference date splits the data into reasonable pre/post periods
- Ensure date columns are properly formatted

### Missing metrics in output
- Verify required columns exist in your CSV files
- Check column names match the expected names

### Workforce mode shows different results
- This is expected! Full workforce includes turnover effects, while common contributors isolates individual impact

## Related Scripts

- `process_devex_metrics.py` - Processes raw GitLab data into normalized CSVs
- `apply_normalization.py` - Normalizes and anonymizes contributor data

## Citation

If you use this script in your research, please cite your paper (to be added after publication).
