#!/usr/bin/env python3
"""
Script otimizado para extrair métricas de múltiplos períodos do Bitbucket
Faz UMA requisição por repositório e filtra localmente para múltiplos períodos
Reduz drasticamente o número de chamadas à API

Períodos analisados:
- Julho 2024
- Agosto 2024
- Setembro 2024
- Julho 2025
- Agosto 2025
- Setembro 2025

Uso:
    python3 analise_detalhada_repos_multiplos_periodos.py
"""

import os
import sys
import csv
from datetime import datetime, timedelta
from collections import defaultdict
import pytz
import requests
from requests.auth import HTTPBasicAuth

# Configurações
BITBUCKET_WORKSPACE = os.getenv("BITBUCKET_WORKSPACE")
BITBUCKET_USERNAME = os.getenv("BITBUCKET_USERNAME", "")
BITBUCKET_API_TOKEN = os.getenv("BITBUCKET_API_TOKEN", "")
BITBUCKET_API_URL = "https://api.bitbucket.org/2.0"

# Definir períodos a serem analisados
PERIODS = [
    {'name': '2024-07', 'start': '2024-07-01', 'end': '2024-07-31'},
    {'name': '2024-08', 'start': '2024-08-01', 'end': '2024-08-31'},
    {'name': '2024-09', 'start': '2024-09-01', 'end': '2024-09-30'},
    {'name': '2025-07', 'start': '2025-07-01', 'end': '2025-07-31'},
    {'name': '2025-08', 'start': '2025-08-01', 'end': '2025-08-31'},
    {'name': '2025-09', 'start': '2025-09-01', 'end': '2025-09-30'},
]


class BitbucketMultiPeriodAnalyzer:
    """Classe para analisar múltiplos períodos com uma única coleta"""
    
    def __init__(self, workspace: str, username: str, api_token: str):
        self.workspace = workspace
        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(username, api_token)
        
        # Cache de dados brutos por repositório
        self.repo_cache = {}
    
    def safe_parse_datetime(self, date_str: str) -> datetime:
        """Parse datetime de forma segura, pegando apenas até os segundos"""
        if not date_str:
            return None
        
        try:
            # Remover 'Z' e substituir por '+00:00' se necessário
            date_str = date_str.replace('Z', '+00:00')
            
            # Pegar apenas até os segundos: YYYY-MM-DDTHH:MM:SS
            # Ignora microssegundos completamente
            if '.' in date_str:
                # Separar a parte antes do ponto (data + hora até segundos)
                date_part = date_str.split('.')[0]
                
                # Extrair timezone se existir
                if '+' in date_str:
                    tz_part = '+' + date_str.split('+')[-1]
                else:
                    tz_part = '+00:00'
                
                # Reconstruir: data completa até segundos + timezone
                date_str = date_part + tz_part
            
            return datetime.fromisoformat(date_str)
            
        except Exception as e:
            print(f"⚠️  Erro ao parsear data '{date_str}': {e}")
            return None
    
    def get_all_commits(self, repo_slug: str) -> list:
        """Buscar TODOS os commits do repositório (uma única vez)"""
        print(f"      Buscando commits...", end=" ", flush=True)
        commits = []
        url = f"{BITBUCKET_API_URL}/repositories/{self.workspace}/{repo_slug}/commits"
        
        try:
            page_count = 0
            while url and page_count < 200:  # Limite de segurança
                response = self.session.get(url, params={'pagelen': 100})
                response.raise_for_status()
                data = response.json()
                page_count += 1
                
                commits.extend(data.get('values', []))
                url = data.get('next')
                
            print(f"✓ {len(commits)} encontrados")
        except Exception as e:
            print(f"⚠️  Erro: {e}")
        
        return commits
    
    def get_all_pull_requests(self, repo_slug: str) -> list:
        """Buscar TODOS os PRs do repositório (todos os estados, uma única vez)"""
        print(f"      Buscando PRs...", end=" ", flush=True)
        all_prs = []
        states = ['OPEN', 'MERGED', 'DECLINED', 'SUPERSEDED']
        
        for state in states:
            try:
                url = f"{BITBUCKET_API_URL}/repositories/{self.workspace}/{repo_slug}/pullrequests"
                page_count = 0
                
                while url and page_count < 100:
                    params = {'state': state, 'pagelen': 50}
                    response = self.session.get(url, params=params if page_count == 0 else None)
                    response.raise_for_status()
                    data = response.json()
                    page_count += 1
                    

                    for pr in data.get('values', []):
                        # Enriquecer PR com informações extras
                        pr_enriched = pr.copy()
                        
                        # Calcular cycle time
                        if pr.get('created_on') and pr.get('updated_on'):
                            created_date = self.safe_parse_datetime(pr['created_on'])
                            updated_date = self.safe_parse_datetime(pr['updated_on'])
                            
                            if created_date and updated_date:
                                cycle_time_hours = (updated_date - created_date).total_seconds() / 3600
                                pr_enriched['cycle_time_hours'] = round(cycle_time_hours, 2)
                        
                        # Time to close para PRs finalizados
                        if pr.get('state') in ['MERGED', 'DECLINED', 'SUPERSEDED']:
                            pr_enriched['time_to_close_hours'] = pr_enriched.get('cycle_time_hours')
                        
                        # Tamanho do PR (simplificado - sem fazer requisição extra)
                        pr_enriched['lines_added'] = 0
                        pr_enriched['lines_removed'] = 0
                        pr_enriched['files_changed'] = 0
                        pr_enriched['total_changes'] = 0

                        
                        url2 = pr['links']['self']['href']
                        response2 = self.session.get(url2)
                        response2.raise_for_status()
                        pr = response2.json()

                        
                        # Reviewers
                        participants = pr.get('participants', [])
                        reviewers = [p for p in participants if str.upper(p.get('role')) in ['REVIEWER', 'PARTICIPANT']]
                        approved_count = sum(1 for p in participants if p.get('approved', False))
                        
                        pr_enriched['total_reviewers'] = len(reviewers)
                        pr_enriched['approved_count'] = approved_count
                        pr_enriched['reviewers_list'] = ', '.join([p.get('user', {}).get('display_name', 'Unknown') for p in reviewers[:5]])
                        
                        all_prs.append(pr_enriched)
                    
                    url = data.get('next')
                    
            except requests.exceptions.HTTPError as e:
                if e.response.status_code != 404:
                    print(f"⚠️  Erro HTTP {state}: {e.response.status_code}")
            except Exception as e:
                print(f"⚠️  Erro {state}: {e}")
        
        print(f"✓ {len(all_prs)} encontrados")
        return all_prs
    
    def get_all_pipelines(self, repo_slug: str) -> list:
        """Buscar TODAS as pipelines do repositório (uma única vez)"""
        print(f"      Buscando pipelines...", end=" ", flush=True)
        pipelines = []
        url = f"{BITBUCKET_API_URL}/repositories/{self.workspace}/{repo_slug}/pipelines/"
        
        try:
            params = {'sort': '-created_on', 'pagelen': 100}
            page_count = 0
            
            while url and page_count < 100:
                response = self.session.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                page_count += 1
                
                for pipeline in data.get('values', []):
                    pipeline_enriched = pipeline.copy()
                    if pipeline.get('created_on') and pipeline.get('completed_on'):
                        created_date = self.safe_parse_datetime(pipeline['created_on'])
                        completed_date = self.safe_parse_datetime(pipeline['completed_on'])
                        
                        if created_date and completed_date:
                            duration_minutes = (completed_date - created_date).total_seconds() / 60
                            pipeline_enriched['duration_minutes'] = round(duration_minutes, 2)
                    
                    pipelines.append(pipeline_enriched)
                
                url = data.get('next')
                if url:
                    params = {}
            
            print(f"✓ {len(pipelines)} encontradas")
        except Exception as e:
            print(f"⚠️  Erro: {e}")
        
        return pipelines
    
    def filter_data_by_period(self, data: list, date_field: str, start_date: datetime, end_date: datetime) -> list:
        """Filtrar dados por período"""
        filtered = []
        
        for item in data:
            date_str = item.get(date_field, '')
            if date_str:
                item_date = self.safe_parse_datetime(date_str)
                if item_date and start_date <= item_date <= end_date:
                    filtered.append(item)
        
        return filtered
    
    def analyze_repository_all_periods(self, repo_slug: str, repo_name: str, project_name: str, periods: list) -> dict:
        """Analisar repositório para todos os períodos de uma vez"""
        print(f"\n{'='*70}")
        print(f"📦 Analisando: {repo_name}")
        print(f"   Projeto: {project_name}")
        print(f"{'='*70}")
        
        # 1. Coletar TODOS os dados uma única vez
   #     all_commits = self.get_all_commits(repo_slug)
        all_prs = self.get_all_pull_requests(repo_slug)
    #    all_pipelines = self.get_all_pipelines(repo_slug)
        
        # 2. Filtrar e analisar para cada período
        results = {}
        
        for period in periods:
            start_date = datetime.strptime(period['start'], '%Y-%m-%d').replace(tzinfo=pytz.UTC)
            end_date = datetime.strptime(period['end'], '%Y-%m-%d').replace(hour=23, minute=59, second=59, tzinfo=pytz.UTC)
            
            # Filtrar dados do período
            # period_commits = self.filter_data_by_period(all_commits, 'date', start_date, end_date)
            period_prs = self.filter_data_by_period(all_prs, 'created_on', start_date, end_date)
            # period_pipelines = self.filter_data_by_period(all_pipelines, 'created_on', start_date, end_date)
            
            # Calcular métricas
            # contributors_data = self.analyze_contributors(period_commits, period_prs)
            pr_metrics = self.calculate_pr_metrics(period_prs)
            # pipeline_metrics = self.calculate_pipeline_metrics(period_pipelines)
            
            results[period['name']] = {
                'repository_name': repo_name,
                'repository_slug': repo_slug,
                'project_name': project_name,
                'period': period['name'],
                'start_date': period['start'],
                'end_date': period['end'],
                # 'total_commits': len(period_commits),
                # 'total_contributors': contributors_data['total_contributors'],
                # 'top_contributor': contributors_data['top_1'][0] if contributors_data['top_1'] else 'N/A',
                # 'top_contributor_commits': contributors_data['top_1'][1]['commits'] if contributors_data['top_1'] else 0,
                # 'top_contributor_prs': contributors_data['top_1'][1]['prs'] if contributors_data['top_1'] else 0,
                'total_prs': pr_metrics['total_prs'],
                'merged_prs': pr_metrics['merged_prs'],
                'open_prs': pr_metrics['open_prs'],
                'declined_prs': pr_metrics['declined_prs'],
                'pr_merge_rate': pr_metrics['merge_rate'],
                'avg_cycle_time_hours': pr_metrics['avg_cycle_time_hours'],
                'median_cycle_time_hours': pr_metrics['median_cycle_time_hours'],
                'avg_time_to_merge_hours': pr_metrics['avg_time_to_merge_hours'],
                # 'total_pipelines': pipeline_metrics['total_pipelines'],
                # 'successful_pipelines': pipeline_metrics['successful_pipelines'],
                # 'failed_pipelines': pipeline_metrics['failed_pipelines'],
                # 'pipeline_success_rate': pipeline_metrics['success_rate'],
                # 'avg_pipeline_duration_minutes': pipeline_metrics['avg_duration_minutes'],
                # 'devex_score': self.calculate_devex_score(pr_metrics, pipeline_metrics, len(period_commits)),
                # # Dados detalhados
                # 'commits': period_commits,
                'prs': period_prs,
                # 'pipelines': period_pipelines
            }
        
        # Imprimir resumo
        # print(f"\n   📈 RESUMO POR PERÍODO:")
        # for period_name, result in results.items():
            # print(f"      {period_name}: {result['total_commits']} commits, {result['total_prs']} PRs, {result['total_pipelines']} pipelines")
        
        return results
    
    def analyze_contributors(self, commits: list, prs: list) -> dict:
        """Analisar contribuidores"""
        contributor_activity = defaultdict(lambda: {'commits': 0, 'prs': 0, 'total': 0})
        
        for commit in commits:
            author_info = commit.get('author', {})
            if 'user' in author_info:
                author = author_info['user'].get('display_name', 'Unknown')
            elif 'raw' in author_info:
                author = author_info['raw']
            else:
                author = 'Unknown'
            
            if author != 'Unknown':
                contributor_activity[author]['commits'] += 1
                contributor_activity[author]['total'] += 1
        
        for pr in prs:
            author = pr.get('author', {}).get('display_name', 'Unknown')
            if author != 'Unknown':
                contributor_activity[author]['prs'] += 1
                contributor_activity[author]['total'] += 2
        
        sorted_contributors = sorted(
            contributor_activity.items(),
            key=lambda x: x[1]['total'],
            reverse=True
        )
        
        return {
            'top_1': sorted_contributors[0] if len(sorted_contributors) > 0 else None,
            'total_contributors': len(contributor_activity)
        }
    
    def calculate_pr_metrics(self, prs: list) -> dict:
        """Calcular métricas de PRs"""
        if not prs:
            return {
                'total_prs': 0, 'merged_prs': 0, 'open_prs': 0, 'declined_prs': 0,
                'avg_cycle_time_hours': 0, 'median_cycle_time_hours': 0,
                'avg_time_to_merge_hours': 0, 'merge_rate': 0
            }
        
        merged_prs = [pr for pr in prs if pr.get('state') == 'MERGED']
        open_prs = [pr for pr in prs if pr.get('state') == 'OPEN']
        declined_prs = [pr for pr in prs if pr.get('state') == 'DECLINED']
        
        cycle_times = [pr['cycle_time_hours'] for pr in prs if pr.get('cycle_time_hours')]
        time_to_merge = [pr['time_to_close_hours'] for pr in merged_prs if pr.get('time_to_close_hours')]
        
        avg_cycle_time = sum(cycle_times) / len(cycle_times) if cycle_times else 0
        median_cycle_time = sorted(cycle_times)[len(cycle_times) // 2] if cycle_times else 0
        avg_time_to_merge = sum(time_to_merge) / len(time_to_merge) if time_to_merge else 0
        merge_rate = (len(merged_prs) / len(prs) * 100) if prs else 0
        
        return {
            'total_prs': len(prs),
            'merged_prs': len(merged_prs),
            'open_prs': len(open_prs),
            'declined_prs': len(declined_prs),
            'avg_cycle_time_hours': round(avg_cycle_time, 2),
            'median_cycle_time_hours': round(median_cycle_time, 2),
            'avg_time_to_merge_hours': round(avg_time_to_merge, 2),
            'merge_rate': round(merge_rate, 2)
        }
    
    def calculate_pipeline_metrics(self, pipelines: list) -> dict:
        """Calcular métricas de pipelines"""
        if not pipelines:
            return {
                'total_pipelines': 0, 'successful_pipelines': 0,
                'failed_pipelines': 0, 'avg_duration_minutes': 0, 'success_rate': 0
            }
        
        successful = sum(1 for p in pipelines if p.get('state', {}).get('result', {}).get('name') == 'SUCCESSFUL')
        failed = sum(1 for p in pipelines if p.get('state', {}).get('result', {}).get('name') == 'FAILED')
        
        durations = [p['duration_minutes'] for p in pipelines if p.get('duration_minutes')]
        avg_duration = sum(durations) / len(durations) if durations else 0
        success_rate = (successful / len(pipelines) * 100) if pipelines else 0
        
        return {
            'total_pipelines': len(pipelines),
            'successful_pipelines': successful,
            'failed_pipelines': failed,
            'avg_duration_minutes': round(avg_duration, 2),
            'success_rate': round(success_rate, 2)
        }
    
    def calculate_devex_score(self, pr_metrics: dict, pipeline_metrics: dict, total_commits: int) -> float:
        """Calcular score de DevEx (0-100)"""
        score = 0
        
        if total_commits > 0:
            score += min(total_commits / 50 * 15, 15)
        
        if pr_metrics['total_prs'] > 0:
            score += min(pr_metrics['total_prs'] / 20 * 15, 15)
        
        if pr_metrics['avg_cycle_time_hours'] > 0:
            score += max(20 - (pr_metrics['avg_cycle_time_hours'] / 168 * 20), 0)
        
        if pipeline_metrics['avg_duration_minutes'] > 0:
            score += max(20 - (pipeline_metrics['avg_duration_minutes'] / 60 * 20), 0)
        
        score += (pr_metrics['merge_rate'] / 100) * 15
        score += (pipeline_metrics['success_rate'] / 100) * 15
        
        return round(score, 1)


def categorize_duration(duration_minutes: float) -> str:
    """Categorizar duração em faixas"""
    if duration_minutes < 5:
        return "Very Short"
    elif duration_minutes < 15:
        return "Short"
    elif duration_minutes < 30:
        return "Medium"
    elif duration_minutes < 60:
        return "Long"
    else:
        return "Very Long"


def read_repos_from_csv(csv_filename: str) -> list:
    """Ler repositórios do CSV"""
    repos = []
    try:
        with open(csv_filename, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                repos.append({
                    'name': row['name'],
                    'slug': row['slug'],
                    'project_name': row['project_name']
                })
    except Exception as e:
        print(f"❌ Erro ao ler CSV: {e}")
        sys.exit(1)
    return repos


def save_period_data(period_name: str, all_results: dict):
    """Salvar dados de um período específico"""
    # CSV de resumo
    summary_data = []
    prs_data = []
    commits_data = []
    pipelines_data = []
    
    for repo_slug, periods_data in all_results.items():
        if period_name in periods_data:
            data = periods_data[period_name]
            
            # Adicionar ao resumo
            summary_data.append({
                'repository_name': data['repository_name'],
                'repository_slug': data['repository_slug'],
                'project_name': data['project_name'],
                'period': data['period'],
                'start_date': data['start_date'],
                'end_date': data['end_date'],
               # 'total_commits': data['total_commits'],
                'total_contributors': data['total_contributors'] if 'total_contributors' in data else 0,
             #   'top_contributor': data['top_contributor'],
              #  'top_contributor_commits': data['top_contributor_commits'],
                'top_contributor_prs': data['top_contributor_prs'] if 'top_contributor_prs' in data else 0,
                'total_prs': data['total_prs'] if 'total_prs' in data else 0,
                'merged_prs': data['merged_prs'] if 'merged_prs' in data else 0,
                'open_prs': data['open_prs'] if 'open_prs' in data else 0,
                'declined_prs': data['declined_prs'] if 'declined_prs' in data else 0,
                'pr_merge_rate': data['pr_merge_rate'] if 'pr_merge_rate' in data else 0,
                'avg_cycle_time_hours': data['avg_cycle_time_hours'] if 'avg_cycle_time_hours' in data else 0,
                'median_cycle_time_hours': data['median_cycle_time_hours'] if 'median_cycle_time_hours' in data else 0,
                'avg_time_to_merge_hours': data['avg_time_to_merge_hours'] if 'avg_time_to_merge_hours' in data else 0,
                'total_pipelines': data['total_pipelines'] if 'total_pipelines' in data else 0,
                'successful_pipelines': data['successful_pipelines'] if 'successful_pipelines' in data else 0,
                'failed_pipelines': data['failed_pipelines'] if 'failed_pipelines' in data else 0,
                'pipeline_success_rate': data['pipeline_success_rate'] if 'pipeline_success_rate' in data else 0,
                'avg_pipeline_duration_minutes': data['avg_pipeline_duration_minutes'] if 'avg_pipeline_duration_minutes' in data else 0,
                'devex_score': data['devex_score'] if 'devex_score' in data else 0
            })
            
            # PRs detalhados
            for pr in data['prs']:
                prs_data.append({
                    'repository_name': data['repository_name'],
                    'repository_slug': data['repository_slug'],
                    'project_name': data['project_name'],
                    'pr_id': pr.get('id'),
                    'pr_title': pr.get('title', ''),
                    'pr_state': pr.get('state', ''),
                    'author': pr.get('author', {}).get('display_name', 'Unknown'),
                    'created_on': pr.get('created_on', ''),
                    'updated_on': pr.get('updated_on', ''),
                    'cycle_time_hours': pr.get('cycle_time_hours', 0),
                    'time_to_close_hours': pr.get('time_to_close_hours', 0),
                    'lines_added': pr.get('lines_added', 0),
                    'lines_removed': pr.get('lines_removed', 0),
                    'files_changed': pr.get('files_changed', 0),
                    'total_changes': pr.get('total_changes', 0),
                    'total_reviewers': pr.get('total_reviewers', 0),
                    'approved_count': pr.get('approved_count', 0),
                    'reviewers_list': pr.get('reviewers_list', ''),
                    'source_branch': pr.get('source', {}).get('branch', {}).get('name', ''),
                    'destination_branch': pr.get('destination', {}).get('branch', {}).get('name', ''),
                    'pr_url': pr.get('links', {}).get('html', {}).get('href', '')
                })
            
            # Commits detalhados
            if 'commits' in data:
                for commit in data['commits']:
                    author_info = commit.get('author', {})
                    if 'user' in author_info:
                        author = author_info['user'].get('display_name', 'Unknown')
                    elif 'raw' in author_info:
                        author = author_info['raw']
                    else:
                        author = 'Unknown'
                    
                    commits_data.append({
                        'repository_name': data['repository_name'],
                        'repository_slug': data['repository_slug'],
                        'project_name': data['project_name'],
                        'commit_hash': commit.get('hash', '')[:8],
                        'commit_hash_full': commit.get('hash', ''),
                        'author': author,
                        'author_email': author_info.get('raw', '').split('<')[-1].replace('>', '').strip() if '<' in author_info.get('raw', '') else '',
                        'message': commit.get('message', '').split('\n')[0][:200],
                        'date': commit.get('date', ''),
                        'commit_url': commit.get('links', {}).get('html', {}).get('href', '')
                    })
                

            if 'pipelines' in data:
                # Pipelines detalhadas - com informações completas sobre falhas
                for pipeline in data['pipelines']:
                    state_info = pipeline.get('state', {})
                    result = state_info.get('result', {})
                    target_info = pipeline.get('target', {})
                    trigger_info = pipeline.get('trigger', {})
                    
                    # Determinar se falhou
                    result_name = result.get('name', 'UNKNOWN')
                    is_failed = result_name in ['FAILED', 'ERROR', 'STOPPED']
                    is_successful = result_name == 'SUCCESSFUL'
                    
                    # Calcular duração
                    duration_minutes = pipeline.get('duration_minutes', 0)
                    
                    # Determinar tipo de falha (se houver)
                    failure_reason = ''
                    if is_failed:
                        if result_name == 'FAILED':
                            failure_reason = 'Build/Test Failed'
                        elif result_name == 'ERROR':
                            failure_reason = 'Pipeline Error'
                        elif result_name == 'STOPPED':
                            failure_reason = 'Manually Stopped'
                    
                    # Obter workspace da URL ou usar padrão
                    workspace = BITBUCKET_WORKSPACE
                    
                    pipelines_data.append({
                        'repository_name': data['repository_name'],
                        'repository_slug': data['repository_slug'],
                        'project_name': data['project_name'],
                        'pipeline_uuid': pipeline.get('uuid', ''),
                        'build_number': pipeline.get('build_number', ''),
                        'state_name': state_info.get('name', ''),
                        'result_name': result_name,
                        'is_successful': 'Yes' if is_successful else 'No',
                        'is_failed': 'Yes' if is_failed else 'No',
                        'failure_reason': failure_reason,
                        'created_on': pipeline.get('created_on', ''),
                        'completed_on': pipeline.get('completed_on', ''),
                        'duration_minutes': duration_minutes,
                        'duration_category': categorize_duration(duration_minutes),
                        'creator': pipeline.get('creator', {}).get('display_name', 'Unknown'),
                        'creator_username': pipeline.get('creator', {}).get('username', ''),
                        'target_branch': target_info.get('ref_name', ''),
                        'target_type': target_info.get('ref_type', ''),
                        'trigger_type': trigger_info.get('name', ''),
                        'commit_hash': target_info.get('commit', {}).get('hash', '')[:8] if target_info.get('commit') else '',
                        'pipeline_url': f"https://bitbucket.org/{workspace}/{data['repository_slug']}/pipelines/results/{pipeline.get('build_number', '')}"
                    })
        
    # Salvar arquivos
    period_clean = period_name.replace('-', '')
    
    if summary_data:
        with open(f"bitbucket_devex_metrics_{period_clean}.csv", 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=summary_data[0].keys())
            writer.writeheader()
            writer.writerows(summary_data)
        print(f"   ✓ Resumo: bitbucket_devex_metrics_{period_clean}.csv")
    
    if prs_data:
        with open(f"bitbucket_prs_details_{period_clean}.csv", 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=prs_data[0].keys())
            writer.writeheader()
            writer.writerows(prs_data)
        print(f"   ✓ PRs: bitbucket_prs_details_{period_clean}.csv ({len(prs_data)} registros)")
    
    if commits_data:
        with open(f"bitbucket_commits_details_{period_clean}.csv", 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=commits_data[0].keys())
            writer.writeheader()
            writer.writerows(commits_data)
        print(f"   ✓ Commits: bitbucket_commits_details_{period_clean}.csv ({len(commits_data)} registros)")
    
    if pipelines_data:
        with open(f"bitbucket_pipelines_details_{period_clean}.csv", 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=pipelines_data[0].keys())
            writer.writeheader()
            writer.writerows(pipelines_data)
        print(f"   ✓ Pipelines: bitbucket_pipelines_details_{period_clean}.csv ({len(pipelines_data)} registros)")


def main():
    """Função principal"""
    print("\n" + "="*70)
    print("🚀 ANÁLISE OTIMIZADA MULTI-PERÍODO - BITBUCKET")
    print("="*70)
    print("\n📅 Períodos que serão analisados:")
    for period in PERIODS:
        print(f"   • {period['name']}: {period['start']} a {period['end']}")
    print()
    
    # Verificar credenciais
    if not BITBUCKET_USERNAME or not BITBUCKET_API_TOKEN:
        print("\n⚠️  ATENÇÃO: Credenciais não configuradas!")
        print("\nConfigure as variáveis de ambiente:")
        print("  export BITBUCKET_WORKSPACE='seu_workspace'")
        print("  export BITBUCKET_USERNAME='seu_usuario'")
        print("  export BITBUCKET_API_TOKEN='seu_api_token'")
        sys.exit(1)
    
    # Buscar arquivo CSV de repositórios
    csv_files = [f for f in os.listdir('.') if f.startswith('bitbucket_repos_ativos_') and f.endswith('.csv')]
    if not csv_files:
        print("\n❌ Nenhum arquivo CSV de repositórios ativos encontrado!")
        sys.exit(1)
    
    csv_filename = sorted(csv_files)[-1]
    print(f"📁 Usando arquivo: {csv_filename}")
    
    # Ler repositórios
    repos = read_repos_from_csv(csv_filename)
    print(f"✓ {len(repos)} repositórios carregados\n")
    
    # Criar analisador
    analyzer = BitbucketMultiPeriodAnalyzer(
        workspace=BITBUCKET_WORKSPACE,
        username=BITBUCKET_USERNAME,
        api_token=BITBUCKET_API_TOKEN
    )
    
    # Analisar cada repositório para todos os períodos
    all_results = {}
    
    try:
        for idx, repo in enumerate(repos, 1):
            print(f"\n[{idx}/{len(repos)}]", end=" ")
            
            try:
                results = analyzer.analyze_repository_all_periods(
                    repo['slug'],
                    repo['name'],
                    repo['project_name'],
                    PERIODS
                )
                all_results[repo['slug']] = results
                
            except KeyboardInterrupt:
                print("\n\n⚠️  Interrompido pelo usuário")
                break
            except Exception as e:
                print(f"\n   ❌ Erro ao analisar repositório: {e}")
                continue
        
        # Salvar resultados por período
        if all_results:
            print(f"\n{'='*70}")
            print("💾 Salvando arquivos por período...")
            print(f"{'='*70}\n")
            
            for period in PERIODS:
                print(f"\n📊 Período {period['name']}:")
                save_period_data(period['name'], all_results)
            
            print(f"\n{'='*70}")
            print("✅ Análise concluída!")
            print(f"{'='*70}")
            print(f"\nTotal de repositórios processados: {len(all_results)}")
            print(f"Total de períodos: {len(PERIODS)}")
            print(f"Total de arquivos gerados: {len(PERIODS) * 4} (resumo + PRs + commits + pipelines)")
        
    except Exception as e:
        print(f"\n❌ Erro durante a análise: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
