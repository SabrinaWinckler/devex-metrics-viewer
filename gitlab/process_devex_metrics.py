#!/usr/bin/env python3
"""
Script para processar dados de DevEx metrics do GitLab
Processa 3 CSVs (mrs.csv, pipelines.csv, commits.csv) e gera métricas agregadas
Baseado em paper acadêmico sobre métricas de Developer Experience
"""

import pandas as pd
import json
import argparse
from datetime import datetime, timedelta
from collections import defaultdict
import os
import glob
from scipy import stats
import numpy as np

def convert_to_native_types(obj):
    """
    Recursivamente converte tipos numpy/pandas para tipos nativos do Python
    para garantir serialização JSON
    """
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

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Processar métricas de DevEx do GitLab',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  # Processar mês/ano específico (busca automaticamente em normalized/)
  python process_devex_metrics.py --month 09 --year 2025
  python process_devex_metrics.py --month 10 --year 2025
  
  # Processar múltiplos meses e anos
  python process_devex_metrics.py --months 07,08,09 --years 2024,2025
  python process_devex_metrics.py --months 07,08,09 --years 2024,2025 --output consolidated_metrics.json
  
  # Especificar arquivos de entrada manualmente
  python process_devex_metrics.py --mrs mrs.csv --pipelines pipelines.csv --commits commits.csv
  
  # Especificar arquivo de saída
  python process_devex_metrics.py --month 09 --year 2025 --output metrics_setembro_2025.json
  
  # Agregação semanal (padrão) ou mensal
  python process_devex_metrics.py --month 09 --year 2025 --period monthly
        """
    )
    
    parser.add_argument('--month', type=str,
                        help='Mês para processar (formato: 01-12 ou janeiro-dezembro)')
    parser.add_argument('--months', type=str,
                        help='Múltiplos meses separados por vírgula (ex: 07,08,09)')
    parser.add_argument('--year', type=str,
                        help='Ano para processar (formato: 2024, 2025)')
    parser.add_argument('--years', type=str,
                        help='Múltiplos anos separados por vírgula (ex: 2024,2025)')
    parser.add_argument('--mrs', type=str,
                        help='Arquivo CSV de Merge Requests (padrão: busca automática)')
    parser.add_argument('--pipelines', type=str,
                        help='Arquivo CSV de Pipelines/Summary (padrão: busca automática)')
    parser.add_argument('--commits', type=str,
                        help='Arquivo CSV de Commits (padrão: busca automática)')
    parser.add_argument('--output', type=str,
                        help='Arquivo JSON de saída (padrão: devex_metrics_<mes>_<ano>.json)')
    parser.add_argument('--period', type=str, choices=['weekly', 'monthly'], default='weekly',
                        help='Período de agregação: weekly ou monthly (padrão: weekly)')
    parser.add_argument('--base-dir', type=str, default='./normalized',
                        help='Diretório base dos arquivos normalized (padrão: ./normalized)')
    
    return parser.parse_args()

def month_name_to_number(month_str):
    """Converte nome do mês para número"""
    months = {
        'janeiro': '01', 'jan': '01',
        'fevereiro': '02', 'fev': '02',
        'marco': '03', 'mar': '03',
        'abril': '04', 'abr': '04',
        'maio': '05', 'mai': '05',
        'junho': '06', 'jun': '06',
        'julho': '07', 'jul': '07',
        'agosto': '08', 'ago': '08',
        'setembro': '09', 'set': '09',
        'outubro': '10', 'out': '10',
        'novembro': '11', 'nov': '11',
        'dezembro': '12', 'dez': '12'
    }
    
    month_lower = month_str.lower().strip()
    
    # Se já for número, retorna com zero à esquerda
    if month_lower.isdigit():
        return month_lower.zfill(2)
    
    return months.get(month_lower, month_str)

def get_month_folder_name(month_num):
    """Retorna o nome da pasta do mês no formato usado"""
    month_folders = {
        '01': '01-janeiro',
        '02': '02-fevereiro',
        '03': '03-marco',
        '04': '04-abril',
        '05': '05-maio',
        '06': '06-junho',
        '07': '07-julho',
        '08': '08-agosto',
        '09': '09-setembro',
        '10': '10-outubro',
        '11': '11-novembro',
        '12': '12-dezembro'
    }
    return month_folders.get(month_num, f'{month_num}-unknown')

def find_latest_file(base_dir, month, year, file_type):
    """
    Busca o arquivo mais recente do tipo especificado para o mês/ano
    file_type: 'commits', 'mrs', ou 'summary'
    """
    month_num = month_name_to_number(month)
    month_folder = get_month_folder_name(month_num)
    
    # Construir padrão de busca
    search_paths = [
        os.path.join(base_dir, month_folder, year, f'gitlab_{file_type}_*.csv'),
        os.path.join(base_dir, month_folder, f'gitlab_{file_type}_*.csv'),
        os.path.join(base_dir, f'gitlab_{file_type}_*.csv')
    ]
    
    all_files = []
    for pattern in search_paths:
        files = glob.glob(pattern)
        # Filtrar arquivos que correspondem ao ano/mês solicitado
        for f in files:
            filename = os.path.basename(f)
            # Exemplo: gitlab_mrs_20250901_to_20250930_20251007_143758.csv
            if year in filename and month_num in filename:
                all_files.append(f)
    
    if not all_files:
        return None
    
    # Retornar o arquivo mais recente (último timestamp)
    return max(all_files, key=os.path.getctime)

def auto_find_files(base_dir, month, year):
    """Busca automaticamente os arquivos commits, mrs e summary/pipelines"""
    print(f"🔍 Buscando arquivos para {month}/{year} em {base_dir}...")
    
    commits_file = find_latest_file(base_dir, month, year, 'commits')
    mrs_file = find_latest_file(base_dir, month, year, 'mrs')
    pipelines_file = find_latest_file(base_dir, month, year, 'pipelines')
    
    if commits_file:
        print(f"   ✓ Commits: {os.path.basename(commits_file)}")
    else:
        print(f"   ⚠️ Commits: não encontrado")
    
    if mrs_file:
        print(f"   ✓ MRs: {os.path.basename(mrs_file)}")
    else:
        print(f"   ⚠️ MRs: não encontrado")
    
    if pipelines_file:
        print(f"   ✓ Summary: {os.path.basename(pipelines_file)}")
    else:
        print(f"   ⚠️ Summary: não encontrado")
    
    return mrs_file, pipelines_file, commits_file

def load_data(mrs_file, pipelines_file, commits_file):
    """Carregar os 3 CSVs"""
    print("\n📂 Carregando arquivos CSV...")
    print(f"\n📍 Caminhos dos arquivos:")
    print(f"   MRs:       {mrs_file if mrs_file else 'Não especificado'}")
    print(f"   Pipelines: {pipelines_file if pipelines_file else 'Não especificado'}")
    print(f"   Commits:   {commits_file if commits_file else 'Não especificado'}")
    print()
    
    try:
        if mrs_file and os.path.exists(mrs_file):
            print(f"   📄 Lendo: {os.path.abspath(mrs_file)}")
            mrs_df = pd.read_csv(mrs_file)
            print(f"   ✓ MRs: {len(mrs_df)} registros")
            if not mrs_df.empty:
                print(f"      Colunas: {', '.join(mrs_df.columns[:10])}{'...' if len(mrs_df.columns) > 10 else ''}")
        else:
            print(f"   ⚠️ MRs não disponível")
            mrs_df = pd.DataFrame()
    except Exception as e:
        print(f"   ⚠️ Erro ao carregar MRs: {e}")
        mrs_df = pd.DataFrame()
    
    try:
        if pipelines_file and os.path.exists(pipelines_file):
            print(f"\n   📄 Lendo: {os.path.abspath(pipelines_file)}")
            pipelines_df = pd.read_csv(pipelines_file)
            print(f"   ✓ Pipelines/Summary: {len(pipelines_df)} registros")
            if not pipelines_df.empty:
                print(f"      Colunas: {', '.join(pipelines_df.columns[:10])}{'...' if len(pipelines_df.columns) > 10 else ''}")
        else:
            print(f"   ⚠️ Pipelines/Summary não disponível")
            pipelines_df = pd.DataFrame()
    except Exception as e:
        print(f"   ⚠️ Erro ao carregar Pipelines: {e}")
        pipelines_df = pd.DataFrame()
    
    try:
        if commits_file and os.path.exists(commits_file):
            print(f"\n   📄 Lendo: {os.path.abspath(commits_file)}")
            commits_df = pd.read_csv(commits_file)
            print(f"   ✓ Commits: {len(commits_df)} registros")
            if not commits_df.empty:
                print(f"      Colunas: {', '.join(commits_df.columns[:10])}{'...' if len(commits_df.columns) > 10 else ''}")
        else:
            print(f"   ⚠️ Commits não disponível")
            commits_df = pd.DataFrame()
    except Exception as e:
        print(f"   ⚠️ Erro ao carregar Commits: {e}")
        commits_df = pd.DataFrame()
    
    return mrs_df, pipelines_df, commits_df

def get_period_label(date, period='weekly'):
    """Gerar label do período com data real"""
    if period == 'weekly':
        # Retorna a data da segunda-feira da semana (início da semana)
        monday = date - timedelta(days=date.weekday())
        return monday.strftime('%Y-%m-%d')
    else:  # monthly
        # Retorna o primeiro dia do mês
        return date.strftime('%Y-%m-01')

def get_period_key(date, period='weekly'):
    """Gerar chave única para agrupar períodos"""
    if period == 'weekly':
        # Usar ano e número da semana ISO
        year, week, _ = date.isocalendar()
        return f"{year}-W{week:02d}"
    else:  # monthly
        # Usar ano e mês
        return date.strftime('%Y-%m')

def process_commit_metrics(commits_df, period='weekly'):
    """
    Processar métricas de commits
    Retorna: commit frequency e code churn por período
    """
    print("\n📊 Processando métricas de commits...")
    
    if commits_df.empty or 'created_at' not in commits_df.columns:
        print("   ⚠️ Dados de commits não disponíveis")
        return []
    
    # Converter created_at para datetime
    commits_df['created_at'] = pd.to_datetime(commits_df['created_at'], errors='coerce')
    commits_df = commits_df.dropna(subset=['created_at'])
    
    # Adicionar coluna de período
    commits_df['period'] = commits_df['created_at'].apply(lambda x: get_period_label(x, period))
    
    # Garantir que lines_added e lines_deleted sejam numéricos
    commits_df['lines_added'] = pd.to_numeric(commits_df['lines_added'], errors='coerce').fillna(0)
    commits_df['lines_deleted'] = pd.to_numeric(commits_df['lines_deleted'], errors='coerce').fillna(0)
    
    # Agregar por período
    commit_data = []
    for period_label in sorted(commits_df['period'].unique()):
        period_commits = commits_df[commits_df['period'] == period_label]
        
        commit_count = len(period_commits)
        churn = int(period_commits['lines_added'].sum() + period_commits['lines_deleted'].sum())
        
        # Coletar contribuidores únicos anonimizados (se disponível)
        contributors = []
        if 'anonymized_name' in period_commits.columns:
            contributors = period_commits['anonymized_name'].dropna().unique().tolist()
        
        commit_data.append({
            'date': period_label,
            'commits': commit_count,
            'churn': churn,
            'project': period_commits['repository_name'].dropna().unique().tolist() if 'repository_name' in period_commits.columns else None,
            'contributors': contributors if contributors else None
        })
    
    print(f"   ✓ {len(commit_data)} períodos processados")
    return commit_data

def process_cicd_metrics(pipelines_df, period='weekly'):
    """
    Processar métricas de CI/CD
    Retorna: pipeline success rate, failure rate e total por período, agrupado por ref (branch/tag)
    """
    print("\n🔧 Processando métricas de CI/CD...")
    
    if pipelines_df.empty:
        print("   ⚠️ Dados de pipelines não disponíveis")
        return []
    
    # Se tiver coluna 'created_at', são dados detalhados de pipelines
    if 'created_at' in pipelines_df.columns:
        pipelines_df['created_at'] = pd.to_datetime(pipelines_df['created_at'], errors='coerce')
        pipelines_df = pipelines_df.dropna(subset=['created_at'])
        
        # Se tiver updated_at, converter também
        if 'updated_at' in pipelines_df.columns:
            pipelines_df['updated_at'] = pd.to_datetime(pipelines_df['updated_at'], errors='coerce')
        
        pipelines_df['period'] = pipelines_df['created_at'].apply(lambda x: get_period_label(x, period))
        
        cicd_data = []
        
        # Agrupar por período e ref (branch/tag)
        for period_label in sorted(pipelines_df['period'].unique()):
            period_pipelines = pipelines_df[pipelines_df['period'] == period_label]
            
            # Se tiver coluna 'ref', agrupar por ela
            if 'ref' in period_pipelines.columns:
                refs_in_period = []
                
                for ref_name in period_pipelines['ref'].unique():
                    ref_pipelines = period_pipelines[period_pipelines['ref'] == ref_name]
                    
                    total = len(ref_pipelines)
                    success = len(ref_pipelines[ref_pipelines['status'] == 'success'])
                    failed = len(ref_pipelines[ref_pipelines['status'] == 'failed'])

                    # Calcular duração em minutos a partir de updated_at e created_at
                    if 'updated_at' in ref_pipelines.columns and 'created_at' in ref_pipelines.columns:
                        duration_minutes = (ref_pipelines['updated_at'] - ref_pipelines['created_at']).dt.total_seconds() / 60
                        duration_minutes = duration_minutes[duration_minutes > 0].dropna()
                    elif 'duration_minutes' in ref_pipelines.columns:
                        duration_minutes = pd.to_numeric(ref_pipelines['duration_minutes'], errors='coerce').dropna()
                        duration_minutes = duration_minutes[duration_minutes > 0]
                    else:
                        duration_minutes = pd.Series([], dtype=float)
                    
                    # Calcular estatísticas de duração
                    avg_duration = round(duration_minutes.mean(), 1) if not duration_minutes.empty else 0
                    max_duration = round(duration_minutes.max(), 1) if not duration_minutes.empty else 0
                    min_duration = round(duration_minutes.min(), 1) if not duration_minutes.empty else 0
                    
                    times_when_duration_time_was_major_than_mean = duration_minutes[duration_minutes > avg_duration]
                    
                    refs_in_period.append({
                        'ref': str(ref_name),
                        'success': success,
                        'failed': failed,
                        'total': total,
                        'avgDuration': avg_duration, 
                        'maxDuration': max_duration,
                        'minDuration': min_duration, 
                        'stdDuration': float(duration_minutes.std() if len(duration_minutes) > 1 else 0),
                        'p95Duration': float(round(np.percentile(duration_minutes, 95), 1) if len(duration_minutes) > 0 else 0),
                        'timesWhenDurationMajorThanMean': int(times_when_duration_time_was_major_than_mean.count() if len(times_when_duration_time_was_major_than_mean) > 0 else 0)
                    })
                
                # Agregar totais do período (todas as refs)
                total = len(period_pipelines)
                success = len(period_pipelines[period_pipelines['status'] == 'success'])
                failed = len(period_pipelines[period_pipelines['status'] == 'failed'])

                # Calcular duração em minutos para todas as refs
                if 'updated_at' in period_pipelines.columns and 'created_at' in period_pipelines.columns:
                    duration_minutes = (period_pipelines['updated_at'] - period_pipelines['created_at']).dt.total_seconds() / 60
                    duration_minutes = duration_minutes[duration_minutes > 0].dropna()
                elif 'duration_minutes' in period_pipelines.columns:
                    duration_minutes = pd.to_numeric(period_pipelines['duration_minutes'], errors='coerce').dropna()
                    duration_minutes = duration_minutes[duration_minutes > 0]
                else:
                    duration_minutes = pd.Series([], dtype=float)
                
                avg_duration = round(duration_minutes.mean(), 1) if not duration_minutes.empty else 0
                max_duration = round(duration_minutes.max(), 1) if not duration_minutes.empty else 0
                min_duration = round(duration_minutes.min(), 1) if not duration_minutes.empty else 0
                
                times_when_duration_time_was_major_than_mean = duration_minutes[duration_minutes > avg_duration]
                 
                cicd_data.append({
                    'date': period_label,
                    'success': success,
                    'failed': failed,
                    'total': total,
                    'avgDuration': avg_duration, 
                    'maxDuration': max_duration,
                    'minDuration': min_duration, 
                    'stdDuration': float(duration_minutes.std() if len(duration_minutes) > 1 else 0),
                    'p95Duration': float(round(np.percentile(duration_minutes, 95), 1) if len(duration_minutes) > 0 else 0),
                    'timesWhenDurationMajorThanMean': int(times_when_duration_time_was_major_than_mean.count() if len(times_when_duration_time_was_major_than_mean) > 0 else 0),
                    'refBreakdown': refs_in_period
                })
            else:
                # Sem coluna ref, processar normalmente
                total = len(period_pipelines)
                success = len(period_pipelines[period_pipelines['status'] == 'success'])
                failed = len(period_pipelines[period_pipelines['status'] == 'failed'])

                # Calcular duração em minutos
                if 'updated_at' in period_pipelines.columns and 'created_at' in period_pipelines.columns:
                    duration_minutes = (period_pipelines['updated_at'] - period_pipelines['created_at']).dt.total_seconds() / 60
                    duration_minutes = duration_minutes[duration_minutes > 0].dropna()
                elif 'duration_minutes' in period_pipelines.columns:
                    duration_minutes = pd.to_numeric(period_pipelines['duration_minutes'], errors='coerce').dropna()
                    duration_minutes = duration_minutes[duration_minutes > 0]
                else:
                    duration_minutes = pd.Series([], dtype=float)
                
                avg_duration = round(duration_minutes.mean(), 1) if not duration_minutes.empty else 0
                max_duration = round(duration_minutes.max(), 1) if not duration_minutes.empty else 0
                min_duration = round(duration_minutes.min(), 1) if not duration_minutes.empty else 0
                
                times_when_duration_time_was_major_than_mean = duration_minutes[duration_minutes > avg_duration]
                
                cicd_data.append({
                    'date': period_label,
                    'success': success,
                    'failed': failed,
                    'total': total,
                    'avgDuration': avg_duration, 
                    'maxDuration': max_duration,
                    'minDuration': min_duration, 
                    'stdDuration': float(duration_minutes.std() if len(duration_minutes) > 1 else 0),
                    'p95Duration': float(round(np.percentile(duration_minutes, 95), 1) if len(duration_minutes) > 0 else 0),
                    'timesWhenDurationMajorThanMean': int(times_when_duration_time_was_major_than_mean.count() if len(times_when_duration_time_was_major_than_mean) > 0 else 0)
                })
        
        # Adicionar total geral
        cicd_data.append({
            'date': 'Total',
            'success': int(len(pipelines_df[pipelines_df['status'] == 'success'])),
            'failed': int(len(pipelines_df[pipelines_df['status'] == 'failed'])),
            'total': int(len(pipelines_df))
        })
        
        print(f"   ✓ {len(cicd_data)-1} períodos + total")
        return cicd_data
    
    # Dados agregados de summary
    elif 'total_pipelines' in pipelines_df.columns:
        print("   ℹ️ Usando dados agregados de summary")
        
        cicd_data = []
        
        # Tentar agrupar por período se tivermos last_activity
        if 'last_activity' in pipelines_df.columns:
            pipelines_df['last_activity'] = pd.to_datetime(pipelines_df['last_activity'], errors='coerce')
            pipelines_df = pipelines_df.dropna(subset=['last_activity'])
            pipelines_df['period'] = pipelines_df['last_activity'].apply(lambda x: x.strftime('%Y-%m-01'))
            
            for period_label in sorted(pipelines_df['period'].unique()):
                period_data = pipelines_df[pipelines_df['period'] == period_label]
                total = int(period_data['total_pipelines'].sum())
                success = int(period_data['pipelines_success'].sum() if 'pipelines_success' in period_data.columns else 0)
                failed = int(period_data['pipelines_failed'].sum() if 'pipelines_failed' in period_data.columns else 0)
                
                # Verificar se tem coluna duration_minutes
                if 'duration_minutes' in period_data.columns:
                    duration_minutes = pd.to_numeric(period_data['duration_minutes'], errors='coerce').dropna()
                    duration_minutes = duration_minutes[duration_minutes > 0]
                else:
                    duration_minutes = pd.Series([], dtype=float)
                
                # Calcular estatísticas de duração
                avg_duration = round(duration_minutes.mean(), 1) if not duration_minutes.empty else 0
                max_duration = round(duration_minutes.max(), 1) if not duration_minutes.empty else 0
                min_duration = round(duration_minutes.min(), 1) if not duration_minutes.empty else 0
                
                times_when_duration_time_was_major_than_mean = duration_minutes[duration_minutes > avg_duration]
                
                if total > 0:
                    cicd_data.append({
                        'date': period_label,
                        'success': success,
                        'failed': failed,
                        'total': total, 
                        'avgDuration': avg_duration, 
                        'maxDuration': max_duration,
                        'minDuration': min_duration, 
                        'stdDuration': float(duration_minutes.std() if len(duration_minutes) > 1 else 0),
                        'p95Duration': float(round(np.percentile(duration_minutes, 95), 1) if len(duration_minutes) > 0 else 0),
                        'timesWhenDurationMajorThanMean': int(times_when_duration_time_was_major_than_mean.count() if len(times_when_duration_time_was_major_than_mean) > 0 else 0)
                    })

        
        
        # Total geral
        cicd_data.append({
            'date': 'Total',
            'success': int(pipelines_df['pipelines_success'].sum() if 'pipelines_success' in pipelines_df.columns else 0),
            'failed': int(pipelines_df['pipelines_failed'].sum() if 'pipelines_failed' in pipelines_df.columns else 0),
            'total': int(pipelines_df['total_pipelines'].sum())
        })
        
        print(f"   ✓ {len(cicd_data)-1} períodos + total")
        return cicd_data
    
    return []

def process_pr_metrics(mrs_df, period='weekly'):
    """
    Processar métricas de Pull Requests (Merge Requests)
    Retorna: PR creation rate, merge time, review participation, e MR code churn
    """
    print("\n🔀 Processando métricas de MRs...")
    
    if mrs_df.empty or 'created_at' not in mrs_df.columns:
        print("   ⚠️ Dados de MRs não disponíveis")
        return []
    
    # Converter datas
    mrs_df['created_at'] = pd.to_datetime(mrs_df['created_at'], errors='coerce')
    if 'merged_at' in mrs_df.columns:
        mrs_df['merged_at'] = pd.to_datetime(mrs_df['merged_at'], errors='coerce')
    mrs_df = mrs_df.dropna(subset=['created_at'])
    
    # Adicionar coluna de período
    mrs_df['period'] = mrs_df['created_at'].apply(lambda x: get_period_label(x, period))
    
    # Garantir que colunas numéricas sejam do tipo correto
    if 'duration_hours' in mrs_df.columns:
        mrs_df['duration_hours'] = pd.to_numeric(mrs_df['duration_hours'], errors='coerce').fillna(0)
    else:
        mrs_df['duration_hours'] = 0
    
    if 'reviewers_count' in mrs_df.columns:
        mrs_df['reviewers_count'] = pd.to_numeric(mrs_df['reviewers_count'], errors='coerce').fillna(0)
    else:
        mrs_df['reviewers_count'] = 0
    
    # Calcular MR Code Churn para cada MR
    mrs_df['mr_churn'] = calculate_mr_code_churn(mrs_df)
    
    # Agregar por período
    pr_data = []
    for period_label in sorted(mrs_df['period'].unique()):
        period_mrs = mrs_df[mrs_df['period'] == period_label]
        
        created = len(period_mrs)
        
        # Contar merged apenas se coluna 'state' existir
        merged = 0
        if 'state' in mrs_df.columns:
            merged = len(period_mrs[period_mrs['state'] == 'merged'])
        
        # Calcular média de tempo de merge (apenas para MRs mergeados)
        avg_merge_time = 0
        if 'state' in mrs_df.columns:
            merged_mrs = period_mrs[period_mrs['state'] == 'merged']
            avg_merge_time = np.median(merged_mrs['duration_hours']) if len(merged_mrs) > 0 else 0
        
        # Calcular média de tempo de review (considerar todos os MRs com duração > 0)
        mrs_with_duration = period_mrs[period_mrs['duration_hours'] > 0]
        avg_review_time = np.median(mrs_with_duration['duration_hours']) if len(mrs_with_duration) > 0 else 0
        
        # Média de reviewers
        avg_reviewers = np.median(period_mrs['reviewers_count']) if len(period_mrs) > 0 else 0
        
        # Calcular métricas de MR Code Churn
        total_mr_churn = period_mrs['mr_churn'].sum()
        avg_mr_churn = period_mrs['mr_churn'].mean() if len(period_mrs) > 0 else 0
        max_mr_churn = period_mrs['mr_churn'].max() if len(period_mrs) > 0 else 0
        min_mr_churn = period_mrs['mr_churn'].min() if len(period_mrs) > 0 else 0
        
        # Calcular componentes individuais do churn para análise
        period_mrs_valid = period_mrs.copy()
        period_mrs_valid['lines_added'] = pd.to_numeric(period_mrs_valid.get('lines_added', 0), errors='coerce').fillna(0)
        period_mrs_valid['lines_deleted'] = pd.to_numeric(period_mrs_valid.get('lines_deleted', 0), errors='coerce').fillna(0)
        period_mrs_valid['files_changed'] = pd.to_numeric(period_mrs_valid.get('files_changed', 0), errors='coerce').fillna(0)
        
        total_lines_added = period_mrs_valid['lines_added'].sum()
        total_lines_deleted = period_mrs_valid['lines_deleted'].sum()
        total_files_changed = period_mrs_valid['files_changed'].sum()
        total_changes = total_lines_added + total_lines_deleted + total_files_changed
        
        # Coletar autores e reviewers anonimizados únicos
        authors = []
        reviewers = []
        if 'anonymized_name' in period_mrs.columns:
            authors = period_mrs['anonymized_name'].dropna().unique().tolist()
        if 'anonymized_reviewers' in mrs_df.columns:
            # Coletar todos os reviewers (podem estar separados por ';')
            all_reviewers = []
            for rev_str in period_mrs['anonymized_reviewers'].dropna():
                if rev_str and isinstance(rev_str, str):
                    all_reviewers.extend([r.strip() for r in rev_str.split(';') if r.strip()])
            reviewers = list(set(all_reviewers))
        
        pr_data.append({
            'date': period_label,
            'created': created,
            'merged': merged,
            'avgReviewTime': round(avg_review_time, 1),
            'avgMergeTime': round(avg_merge_time, 1),
            'reviewers': round(avg_reviewers, 1),
            'authors': authors if authors else None,
            'reviewersList': reviewers if reviewers else None,
            # Novas métricas de MR Code Churn
            'mrChurn': {
                'total': round(total_mr_churn, 2),
                'average': round(avg_mr_churn, 2),
                'maximum': round(max_mr_churn, 2),
                'minimum': round(min_mr_churn, 2),
                # Componentes detalhados
                'components': {
                    'totalLinesAdded': int(total_lines_added),
                    'totalLinesDeleted': int(total_lines_deleted),
                    'totalFilesChanged': int(total_files_changed),
                    'totalChanges': int(total_changes)
                }
            }
        })
    
    print(f"   ✓ {len(pr_data)} períodos processados com métricas de MR Code Churn")
    return pr_data

def calculate_summary_stats(mrs_df, pipelines_df, commits_df):
    """
    Calcular estatísticas gerais (summaryStats) separadas por ano
    """
    print("\n📈 Calculando estatísticas gerais...")
    
    summary = {
        'overall': {
            'totalCommits': 0,
            'totalMRs': 0,
            'avgMergeRate': 0,
            'avgPipelineSuccess': 0,
            'avgCycleTime': 0,
            'activeRepos': 0
        },
        'byYear': {}
    }
    
    # Identificar anos disponíveis dos commits e MRs
    years = set()
    
    if not commits_df.empty and 'created_at' in commits_df.columns:
        commits_df['created_at'] = pd.to_datetime(commits_df['created_at'], errors='coerce')
        commits_df['year'] = commits_df['created_at'].dt.year
        years.update(commits_df['year'].dropna().unique())
    
    if not mrs_df.empty and 'created_at' in mrs_df.columns:
        mrs_df['created_at'] = pd.to_datetime(mrs_df['created_at'], errors='coerce')
        mrs_df['year'] = mrs_df['created_at'].dt.year
        years.update(mrs_df['year'].dropna().unique())
    
    # Para dados de pipelines agregados, calcular totais gerais primeiro
    total_pipelines_all = 0
    total_success_all = 0
    if not pipelines_df.empty and 'total_pipelines' in pipelines_df.columns:
        total_pipelines_all = int(pipelines_df['total_pipelines'].sum())
        total_success_all = int(pipelines_df['pipelines_success'].sum() if 'pipelines_success' in pipelines_df.columns else 0)
    
    # Calcular estatísticas por ano
    for year in sorted(years):
        year_str = str(int(year))
        
        # Filtrar dados do ano
        year_commits = commits_df[commits_df['year'] == year].copy() if not commits_df.empty and 'year' in commits_df.columns else pd.DataFrame()
        year_mrs = mrs_df[mrs_df['year'] == year].copy() if not mrs_df.empty and 'year' in mrs_df.columns else pd.DataFrame()
        
        year_stats = {
            'totalCommits': 0,
            'totalMRs': 0,
            'avgMergeRate': 0,
            'avgPipelineSuccess': 0,
            'avgCycleTime': 0,
            'activeRepos': 0
        }
        
        # Total de commits do ano
        if not year_commits.empty:
            year_stats['totalCommits'] = int(len(year_commits))
        
        # Total de MRs e taxa de merge do ano
        if not year_mrs.empty:
            year_stats['totalMRs'] = int(len(year_mrs))
            
            if 'state' in year_mrs.columns:
                merged_count = len(year_mrs[year_mrs['state'] == 'merged'])
                if year_stats['totalMRs'] > 0:
                    year_stats['avgMergeRate'] = round((merged_count / year_stats['totalMRs']) * 100, 1)
            
            # Cycle time médio do ano
            if 'duration_hours' in year_mrs.columns:
                year_mrs['duration_hours'] = pd.to_numeric(year_mrs['duration_hours'], errors='coerce').fillna(0)
                
                if 'state' in year_mrs.columns:
                    merged_mrs = year_mrs[year_mrs['state'] == 'merged']
                    if len(merged_mrs) > 0:
                        year_stats['avgCycleTime'] = round(merged_mrs['duration_hours'].mean(), 1)
                else:
                    mrs_with_duration = year_mrs[year_mrs['duration_hours'] > 0]
                    if len(mrs_with_duration) > 0:
                        year_stats['avgCycleTime'] = round(mrs_with_duration['duration_hours'].mean(), 1)
        
        # Pipeline success rate do ano
        # Para dados agregados (summary), calcular proporcionalmente baseado em commits
        if not pipelines_df.empty and 'total_pipelines' in pipelines_df.columns:
            # Pegar repositórios ativos do ano
            year_repos = set()
            if not year_commits.empty and 'repository' in year_commits.columns:
                year_repos.update(year_commits['repository'].unique())
            if not year_mrs.empty and 'repository' in year_mrs.columns:
                year_repos.update(year_mrs['repository'].unique())
            
            # Filtrar dados de pipeline dos repositórios do ano
            if len(year_repos) > 0 and 'repository' in pipelines_df.columns:
                year_pipelines = pipelines_df[pipelines_df['repository'].isin(year_repos)]
                
                total_pipes = year_pipelines['total_pipelines'].sum()
                success_pipes = year_pipelines['pipelines_success'].sum() if 'pipelines_success' in year_pipelines.columns else 0
                
                if total_pipes > 0:
                    year_stats['avgPipelineSuccess'] = round((success_pipes / total_pipes) * 100, 1)
        
        # Repositórios ativos do ano
        active_repos_year = set()
        if not year_commits.empty and 'repository' in year_commits.columns:
            active_repos_year.update(year_commits['repository'].unique())
        if not year_mrs.empty and 'repository' in mrs_df.columns:
            active_repos_year.update(mrs_df['repository'].unique())
        
        year_stats['activeRepos'] = len(active_repos_year)
        
        summary['byYear'][year_str] = year_stats
        print(f"   ✓ Estatísticas de {year_str} calculadas")
    
    # Calcular estatísticas gerais (overall)
    summary['overall']['totalCommits'] = int(len(commits_df)) if not commits_df.empty else 0
    summary['overall']['totalMRs'] = int(len(mrs_df)) if not mrs_df.empty else 0
    
    # Taxa de merge geral
    if not mrs_df.empty and 'state' in mrs_df.columns:
        merged_count = len(mrs_df[mrs_df['state'] == 'merged'])
        if summary['overall']['totalMRs'] > 0:
            summary['overall']['avgMergeRate'] = round((merged_count / summary['overall']['totalMRs']) * 100, 1)
    
    # Cycle time médio geral
    if not mrs_df.empty and 'duration_hours' in mrs_df.columns:
        mrs_df['duration_hours'] = pd.to_numeric(mrs_df['duration_hours'], errors='coerce').fillna(0)
        
        if 'state' in mrs_df.columns:
            merged_mrs = mrs_df[mrs_df['state'] == 'merged']
            if len(merged_mrs) > 0:
                summary['overall']['avgCycleTime'] = round(merged_mrs['duration_hours'].mean(), 1)
        else:
            mrs_with_duration = mrs_df[mrs_df['duration_hours'] > 0]
            if len(mrs_with_duration) > 0:
                summary['overall']['avgCycleTime'] = round(mrs_with_duration['duration_hours'].mean(), 1)
    
    # Pipeline success rate geral
    if total_pipelines_all > 0:
        summary['overall']['avgPipelineSuccess'] = round((total_success_all / total_pipelines_all) * 100, 1)
    
    # Repositórios ativos geral
    active_repos = set()
    if not commits_df.empty and 'repository' in commits_df.columns:
        active_repos.update(commits_df['repository'].unique())
    if not mrs_df.empty and 'repository' in mrs_df.columns:
        active_repos.update(mrs_df['repository'].unique())
    if not pipelines_df.empty and 'repository' in pipelines_df.columns:
        active_repos.update(pipelines_df['repository'].unique())
    
    summary['overall']['activeRepos'] = len(active_repos)
    
    print(f"   ✓ Estatísticas gerais calculadas")
    return summary

def calculate_repo_breakdown(mrs_df, pipelines_df, commits_df):
    """
    Calcular breakdown por repositório
    """
    print("\n📦 Calculando breakdown por repositório...")
    
    # Coletar todos os repositórios únicos
    all_repos = set()
    if not commits_df.empty and 'repository' in commits_df.columns:
        all_repos.update(commits_df['repository'].unique())
    if not mrs_df.empty and 'repository' in mrs_df.columns:
        all_repos.update(mrs_df['repository'].unique())
    if not pipelines_df.empty and 'repository' in pipelines_df.columns:
        all_repos.update(pipelines_df['repository'].unique())
    
    repo_breakdown = []
    
    for repo in sorted(all_repos):
        # Commits do repo
        repo_commits = len(commits_df[commits_df['repository'] == repo]) if not commits_df.empty and 'repository' in commits_df.columns else 0
        
        # MRs do repo
        repo_mrs = len(mrs_df[mrs_df['repository'] == repo]) if not mrs_df.empty and 'repository' in mrs_df.columns else 0
        
        # Pipeline success rate do repo
        pipeline_success = 0
        top_contributor = None
        
        if not pipelines_df.empty and 'repository' in pipelines_df.columns:
            repo_pipelines = pipelines_df[pipelines_df['repository'] == repo]
            
            if 'status' in pipelines_df.columns:
                # Dados detalhados
                total = len(repo_pipelines)
                success = len(repo_pipelines[repo_pipelines['status'] == 'success'])
                if total > 0:
                    pipeline_success = round((success / total) * 100, 1)
            elif 'total_pipelines' in pipelines_df.columns and len(repo_pipelines) > 0:
                # Dados agregados (pegar primeira linha do repo)
                repo_data = repo_pipelines.iloc[0]
                total_pipes = repo_data.get('total_pipelines', 0)
                success_pipes = repo_data.get('pipelines_success', 0)
                if total_pipes > 0:
                    pipeline_success = round((success_pipes / total_pipes) * 100, 1)
                
                # Pegar top contributor anonimizado se disponível
                if 'anonymized_top_contributor' in repo_data:
                    top_contributor = repo_data.get('anonymized_top_contributor')
        
        # Pegar nome do repositório (último segmento do path)
        repo_name = repo.split('/')[-1] if '/' in repo else repo
        
        repo_entry = {
            'repo': repo_name,
            'commits': repo_commits,
            'mrs': repo_mrs,
            'pipelineSuccess': pipeline_success
        }
        
        # Adicionar top contributor se disponível
        if top_contributor:
            repo_entry['topContributor'] = top_contributor
        
        repo_breakdown.append(repo_entry)
    
    # Ordenar por número de commits (decrescente)
    repo_breakdown.sort(key=lambda x: x['commits'], reverse=True)
    
    print(f"   ✓ {len(repo_breakdown)} repositórios processados")
    return repo_breakdown

# Define constant for anonymized placeholder
NA_CONTRIBUTOR = 'P n/a'

def analyse_pr_and_commit_descriptions(mrs_df, commits_df):
    """
    Analisar descrições de MRs e commits para identificar padrões comuns
    Análise separada por ano (2024 e 2025)
    Inclui lista de pessoas anonimizadas (Pn) que contribuíram em cada padrão
    """
    print("\n🔍 Analisando padrões de descrições por ano...")
    
    result = {
        'mrs_analysis': {},
        'commits_analysis': {}
    }
    
    # Padrões de palavras-chave (comum para commits)
    pattern_keywords = {
        'fix/bug/issue': ['fix', 'bug', 'issue', 'hotfix', 'bugfix'],
        'feature/add/new/feat': ['feature', 'add', 'new', 'feat', 'implement'],
        'doc/documentation/readme': ['doc', 'documentation', 'readme', 'docs'],
        'test/testing/ci/coverage': ['test', 'testing', 'ci', 'coverage', 'spec'],
        'refactor/cleanup/clean/restructure': ['refactor', 'cleanup', 'clean', 'restructure', 'improve'],
        'update/upgrade/bump/version': ['update', 'upgrade', 'bump', 'version', 'migrate'],
        'chore/misc/miscellaneous': ['chore', 'misc', 'miscellaneous', 'merge', 'style']
    }
    
    # Análise de Commits separada por ano
    if not commits_df.empty and 'message' in commits_df.columns:
        # Converter created_at para datetime e adicionar coluna de ano
        commits_df['created_at'] = pd.to_datetime(commits_df['created_at'], errors='coerce')
        commits_df['year'] = commits_df['created_at'].dt.year
        
        # Análise por ano
        commits_by_year = {}
        
        for year in sorted(commits_df['year'].dropna().unique()):
            year_commits = commits_df[commits_df['year'] == year].copy()
            
            # Contadores de padrões e contribuidores para o ano
            pattern_counts = {
                'fix/bug/issue': {'count': 0, 'contributors': set(), 'codeChurn':0, 'netChange':0, 'dates': []},
                'feature/add/new/feat': {'count': 0, 'contributors': set(), 'codeChurn':0, 'netChange':0, 'dates': []},
                'doc/documentation/readme': {'count': 0, 'contributors': set(), 'codeChurn':0, 'netChange':0,'dates': []},
                'test/testing/ci/coverage': {'count': 0, 'contributors': set(), 'codeChurn':0, 'netChange':0,'dates': []},
                'refactor/cleanup/clean/restructure': {'count': 0, 'contributors': set(), 'codeChurn':0, 'netChange':0,'dates': []},
                'update/upgrade/bump/version': {'count': 0, 'contributors': set(), 'codeChurn':0, 'netChange':0,'dates': []},
                'chore/misc/miscellaneous': {'count': 0, 'contributors': set(),'codeChurn':0, 'netChange':0, 'dates': []},
                'other': {'count': 0, 'contributors': set(), 'codeChurn':0, 'netChange':0, 'dates': []}
            }
            
            # Analisar cada mensagem com seu autor
            for idx, row in year_commits.iterrows():
                message = row.get('message')
                author = row.get('anonymized_name')
                date = row.get('created_at')

                churn = int(row['lines_added'] + row['lines_deleted'])
                net_change = int(row['lines_added'] - row['lines_deleted'])
                
                if pd.notna(message) and isinstance(message, str) and pd.notna(author) and author != NA_CONTRIBUTOR:
                    message_lower = message.lower()
                    pattern_found = False
                    
                    # Verificar cada padrão
                    for pattern_name, keywords in pattern_keywords.items():
                        if any(keyword in message_lower for keyword in keywords):
                            pattern_counts[pattern_name]['codeChurn'] += churn
                            pattern_counts[pattern_name]['netChange'] += net_change
                            pattern_counts[pattern_name]['count'] += 1
                            pattern_counts[pattern_name]['contributors'].add(author)
                            pattern_counts[pattern_name]['dates'].append(date)
                            pattern_found = True
                            break  # Considerar apenas o primeiro padrão encontrado
                    
                    # Se nenhum padrão foi encontrado, classificar como 'other'
                    if not pattern_found:
                        pattern_counts['other']['count'] += 1
                        pattern_counts['other']['contributors'].add(author)
                        pattern_counts['other']['codeChurn'] += churn
                        pattern_counts['other']['netChange'] += net_change   
                        pattern_counts['other']['dates'].append(date)
            
            # Converter para lista de dicionários
            description_patterns = []
            for pattern_name, data in pattern_counts.items():
                if data['count'] > 0:
                    # Pegar data mais recente do padrão
                    latest_date = max(data['dates']) if data['dates'] else None
                    
                    description_patterns.append({
                        'pattern': pattern_name,
                        'count': int(data['count']),
                        'codeChurn':  data['codeChurn'] if 'codeChurn' in data else 0,  
                        'netChange': data['netChange'] if 'netChange' in data else 0,
                        'contributors': sorted(list(data['contributors'])),
                        'latestDate': latest_date.strftime('%Y-%m-%d') if latest_date and pd.notna(latest_date) else None
                    })
            
            year_str = str(int(year))
            commits_by_year[year_str] = {
                'totalAnalyzed': int(year_commits['message'].notna().sum()),
                'patterns': description_patterns
            }
            
            print(f"   ✓ Commits {year_str}: {commits_by_year[year_str]['totalAnalyzed']} analisados")
        
        result['commits_analysis'] = {
            'byYear': commits_by_year,
            'totalAnalyzed': int(commits_df['message'].notna().sum())
        }
    
    print(f"   ✓ Total de commits analisados: {result.get('commits_analysis', {}).get('totalAnalyzed', 0)}")
    
    return result

def perform_mann_whitney_tests_with_full_workforce(commits_df, mrs_df, pipelines_df, reference_date=None):
    """
    Realiza testes Mann-Whitney comparando períodos antes e depois de uma data de referência
    Utiliza TODA a força de trabalho (todos os contribuidores), mesmo que não estejam em ambos os períodos
    Se reference_date não for fornecida, usa o meio do período como referência
    
    Métricas testadas:
    1. Commit Frequency
    2. MR/PR Creation Frequency
    3. Merge Time
    4. Pipeline Time Avg
    """
    print("\n📊 Executando testes estatísticos Mann-Whitney (Full Workforce)...")
    
    results = {}
    
    # Se não foi fornecida data de referência, usar o meio do período
    if reference_date is None:
        # Pegar todas as datas disponíveis
        all_dates = []
        if not commits_df.empty and 'created_at' in commits_df.columns:
            all_dates.extend(pd.to_datetime(commits_df['created_at'], errors='coerce').dropna().tolist())
        if not mrs_df.empty and 'created_at' in mrs_df.columns:
            all_dates.extend(pd.to_datetime(mrs_df['created_at'], errors='coerce').dropna().tolist())
        
        if all_dates:
            min_date = min(all_dates)
            max_date = max(all_dates)
            reference_date = min_date + (max_date - min_date) / 2
            print(f"   ℹ️ Data de referência calculada: {reference_date.strftime('%Y-%m-%d')}")
        else:
            print("   ⚠️ Não foi possível calcular data de referência")
            return results
    else:
        reference_date = pd.to_datetime(reference_date)
        print(f"   ℹ️ Usando data de referência: {reference_date.strftime('%Y-%m-%d')}")
    
    # 1. Teste para Commit Frequency
    if not commits_df.empty and 'created_at' in commits_df.columns:
        commits_df['created_at'] = pd.to_datetime(commits_df['created_at'], errors='coerce')
        commits_df = commits_df.dropna(subset=['created_at'])
        
        # Agrupar por semana
        commits_df['week'] = commits_df['created_at'].dt.to_period('W')
        commits_per_week = commits_df.groupby('week').size().reset_index(name='count')
        
        # Separar em pre e post
        commits_per_week['timestamp'] = commits_per_week['week'].apply(lambda x: x.to_timestamp())
        pre_commits = commits_per_week[commits_per_week['timestamp'] < reference_date]['count'].values
        post_commits = commits_per_week[commits_per_week['timestamp'] >= reference_date]['count'].values
        
        if len(pre_commits) > 0 and len(post_commits) > 0:
            # Coletar todos os contribuidores (full workforce)
            all_contributors_pre = set()
            all_contributors_post = set()
            if 'anonymized_name' in commits_df.columns:
                commits_df_clean = commits_df[commits_df['anonymized_name'] != 'P n/a']
                pre_commits_df = commits_df_clean[commits_df_clean['created_at'] < reference_date]
                post_commits_df = commits_df_clean[commits_df_clean['created_at'] >= reference_date]
                all_contributors_pre = set(pre_commits_df['anonymized_name'].unique())
                all_contributors_post = set(post_commits_df['anonymized_name'].unique())
            
            results['commitFrequency'] = perform_mann_whitney(
                pre_commits, 
                post_commits, 
                "Commit Frequency",
                all_contributors_pre=list(all_contributors_pre),
                all_contributors_post=list(all_contributors_post)
            )
            print(f"   ✓ Commit Frequency: p-value={results['commitFrequency']['pValue']:.4f}")
    
    # 2. Teste para MR/PR Creation Frequency
    if not mrs_df.empty and 'created_at' in mrs_df.columns:
        mrs_df['created_at'] = pd.to_datetime(mrs_df['created_at'], errors='coerce')
        mrs_df = mrs_df.dropna(subset=['created_at'])
        
        # Agrupar por semana
        mrs_df['week'] = mrs_df['created_at'].dt.to_period('W')
        mrs_per_week = mrs_df.groupby('week').size().reset_index(name='count')
        
        # Separar em pre e post
        mrs_per_week['timestamp'] = mrs_per_week['week'].apply(lambda x: x.to_timestamp())
        pre_mrs = mrs_per_week[mrs_per_week['timestamp'] < reference_date]['count'].values
        post_mrs = mrs_per_week[mrs_per_week['timestamp'] >= reference_date]['count'].values
        
        if len(pre_mrs) > 0 and len(post_mrs) > 0:
            # Coletar todos os autores (full workforce)
            all_authors_pre = set()
            all_authors_post = set()
            if 'anonymized_name' in mrs_df.columns:
                mrs_df_clean = mrs_df[mrs_df['anonymized_name'] != 'P n/a']
                pre_mrs_df = mrs_df_clean[mrs_df_clean['created_at'] < reference_date]
                post_mrs_df = mrs_df_clean[mrs_df_clean['created_at'] >= reference_date]
                all_authors_pre = set(pre_mrs_df['anonymized_name'].unique())
                all_authors_post = set(post_mrs_df['anonymized_name'].unique())
            
            results['mrFrequency'] = perform_mann_whitney(
                pre_mrs, 
                post_mrs, 
                "MR/PR Creation Frequency",
                all_contributors_pre=list(all_authors_pre),
                all_contributors_post=list(all_authors_post)
            )
            print(f"   ✓ MR/PR Frequency: p-value={results['mrFrequency']['pValue']:.4f}")
    
    # 3. Teste para Merge Time
    if not mrs_df.empty and 'duration_hours' in mrs_df.columns and 'state' in mrs_df.columns:
        merged_mrs = mrs_df[mrs_df['state'] == 'merged'].copy()
        merged_mrs['duration_hours'] = pd.to_numeric(merged_mrs['duration_hours'], errors='coerce')
        merged_mrs = merged_mrs.dropna(subset=['duration_hours'])
        
        if not merged_mrs.empty:
            # Separar em pre e post
            pre_merge_time = merged_mrs[merged_mrs['created_at'] < reference_date]['duration_hours'].values
            post_merge_time = merged_mrs[merged_mrs['created_at'] >= reference_date]['duration_hours'].values
            
            if len(pre_merge_time) > 0 and len(post_merge_time) > 0:
                # Coletar todos os autores
                all_merge_authors_pre = set()
                all_merge_authors_post = set()
                if 'anonymized_name' in merged_mrs.columns:
                    merged_mrs_clean = merged_mrs[merged_mrs['anonymized_name'] != 'P n/a']
                    pre_merge_df = merged_mrs_clean[merged_mrs_clean['created_at'] < reference_date]
                    post_merge_df = merged_mrs_clean[merged_mrs_clean['created_at'] >= reference_date]
                    all_merge_authors_pre = set(pre_merge_df['anonymized_name'].unique())
                    all_merge_authors_post = set(post_merge_df['anonymized_name'].unique())
                
                results['mergeTime'] = perform_mann_whitney(
                    pre_merge_time, 
                    post_merge_time, 
                    "Merge Time (hours)",
                    all_contributors_pre=list(all_merge_authors_pre),
                    all_contributors_post=list(all_merge_authors_post)
                )
                print(f"   ✓ Merge Time: p-value={results['mergeTime']['pValue']:.4f}")
    
    # 4. Teste para Pipeline Time Avg
    if not pipelines_df.empty:
        if 'created_at' in pipelines_df.columns and 'updated_at' in pipelines_df.columns:
            pipelines_df['created_at'] = pd.to_datetime(pipelines_df['created_at'], errors='coerce')
            pipelines_df['updated_at'] = pd.to_datetime(pipelines_df['updated_at'], errors='coerce')
            pipelines_df = pipelines_df.dropna(subset=['created_at', 'updated_at'])
            
            # Calcular duração em minutos
            pipelines_df['duration_minutes'] = (pipelines_df['updated_at'] - pipelines_df['created_at']).dt.total_seconds() / 60
            pipelines_df = pipelines_df[pipelines_df['duration_minutes'] > 0]
            
            if not pipelines_df.empty:
                # Separar em pre e post
                pre_pipeline_time = pipelines_df[pipelines_df['created_at'] < reference_date]['duration_minutes'].values
                post_pipeline_time = pipelines_df[pipelines_df['created_at'] >= reference_date]['duration_minutes'].values
                
                if len(pre_pipeline_time) > 0 and len(post_pipeline_time) > 0:
                    results['pipelineTimeAvg'] = perform_mann_whitney(
                        pre_pipeline_time, 
                        post_pipeline_time, 
                        "Pipeline Time Avg (minutes)"
                    )
                    print(f"   ✓ Pipeline Time Avg: p-value={results['pipelineTimeAvg']['pValue']:.4f}")
    
    print(f"   ✓ {len(results)} testes estatísticos realizados (Full Workforce)")
    return results


def perform_mann_whitney_tests_with_common_persons_only(commits_df, mrs_df, pipelines_df, reference_date=None):
    """
    Realiza testes Mann-Whitney comparando períodos antes e depois de uma data de referência
    Compara apenas os mesmos contribuidores (anonymized_names) que aparecem em ambos os períodos
    Se reference_date não for fornecida, usa o meio do período como referência
    
    Métricas testadas:
    1. Commit Frequency
    2. MR/PR Creation Frequency
    3. Merge Time
    4. Pipeline Time Avg
    """
    print("\n📊 Executando testes estatísticos Mann-Whitney (Common Persons Only)...")
    
    results = {}
    
    # Se não foi fornecida data de referência, usar o meio do período
    if reference_date is None:
        # Pegar todas as datas disponíveis
        all_dates = []
        if not commits_df.empty and 'created_at' in commits_df.columns:
            all_dates.extend(pd.to_datetime(commits_df['created_at'], errors='coerce').dropna().tolist())
        if not mrs_df.empty and 'created_at' in mrs_df.columns:
            all_dates.extend(pd.to_datetime(mrs_df['created_at'], errors='coerce').dropna().tolist())
        
        if all_dates:
            min_date = min(all_dates)
            max_date = max(all_dates)
            reference_date = min_date + (max_date - min_date) / 2
            print(f"   ℹ️ Data de referência calculada: {reference_date.strftime('%Y-%m-%d')}")
        else:
            print("   ⚠️ Não foi possível calcular data de referência")
            return results
    else:
        reference_date = pd.to_datetime(reference_date)
        print(f"   ℹ️ Usando data de referência: {reference_date.strftime('%Y-%m-%d')}")
    
    # 1. Teste para Commit Frequency - por contribuidor
    if not commits_df.empty and 'created_at' in commits_df.columns and 'anonymized_name' in commits_df.columns:
        commits_df['created_at'] = pd.to_datetime(commits_df['created_at'], errors='coerce')
        commits_df = commits_df.dropna(subset=['created_at', 'anonymized_name'])
        
        # Filtrar apenas contribuidores válidos (não P n/a)
        commits_df = commits_df[commits_df['anonymized_name'] != 'P n/a']
        
        # Filtrar apenas contribuidores válidos (não P n/a)
        commits_df = commits_df[commits_df['anonymized_name'] != NA_CONTRIBUTOR]
        commits_per_week_person = commits_df.groupby(['week', 'anonymized_name']).size().reset_index(name='count')
        
        # Separar em pre e post
        commits_per_week_person['timestamp'] = commits_per_week_person['week'].apply(lambda x: x.to_timestamp())
        pre_commits_df = commits_per_week_person[commits_per_week_person['timestamp'] < reference_date]
        post_commits_df = commits_per_week_person[commits_per_week_person['timestamp'] >= reference_date]
        
        # Encontrar contribuidores comuns em ambos os períodos
        pre_contributors = set(pre_commits_df['anonymized_name'].unique())
        post_contributors = set(post_commits_df['anonymized_name'].unique())
        common_contributors = pre_contributors & post_contributors
        
        if len(common_contributors) > 0:
            print(f"   ℹ️ Commits: {len(common_contributors)} contribuidores comuns em ambos os períodos")
            
            # Filtrar apenas contribuidores comuns
            pre_commits_filtered = pre_commits_df[pre_commits_df['anonymized_name'].isin(common_contributors)]
            post_commits_filtered = post_commits_df[post_commits_df['anonymized_name'].isin(common_contributors)]
            
            # Agrupar por semana (somar commits de todos os contribuidores comuns)
            pre_commits = pre_commits_filtered.groupby('week')['count'].sum().values
            post_commits = post_commits_filtered.groupby('week')['count'].sum().values
            
            if len(pre_commits) > 0 and len(post_commits) > 0:
                results['commitFrequency'] = perform_mann_whitney(
                    pre_commits, 
                    post_commits, 
                    "Commit Frequency",
                    common_contributors=list(common_contributors)
                )
                print(f"   ✓ Commit Frequency: p-value={results['commitFrequency']['pValue']:.4f}")
        else:
            print("   ⚠️ Nenhum contribuidor comum encontrado para Commit Frequency")
    
    # 2. Teste para MR/PR Creation Frequency - por autor
    if not mrs_df.empty and 'created_at' in mrs_df.columns and 'anonymized_name' in mrs_df.columns:
        mrs_df['created_at'] = pd.to_datetime(mrs_df['created_at'], errors='coerce')
        mrs_df = mrs_df.dropna(subset=['created_at', 'anonymized_name'])
        
        # Filtrar apenas autores válidos (não P n/a)
        mrs_df = mrs_df[mrs_df['anonymized_name'] != 'P n/a']
        
        # Filtrar apenas autores válidos (não P n/a)
        mrs_df = mrs_df[mrs_df['anonymized_name'] != NA_CONTRIBUTOR]
        mrs_per_week_person = mrs_df.groupby(['week', 'anonymized_name']).size().reset_index(name='count')
        
        # Separar em pre e post
        mrs_per_week_person['timestamp'] = mrs_per_week_person['week'].apply(lambda x: x.to_timestamp())
        pre_mrs_df = mrs_per_week_person[mrs_per_week_person['timestamp'] < reference_date]
        post_mrs_df = mrs_per_week_person[mrs_per_week_person['timestamp'] >= reference_date]
        
        # Encontrar autores comuns em ambos os períodos
        pre_authors = set(pre_mrs_df['anonymized_name'].unique())
        post_authors = set(post_mrs_df['anonymized_name'].unique())
        common_authors = pre_authors & post_authors
        
        if len(common_authors) > 0:
            print(f"   ℹ️ MR/PR: {len(common_authors)} autores comuns em ambos os períodos")
            
            # Filtrar apenas autores comuns
            pre_mrs_filtered = pre_mrs_df[pre_mrs_df['anonymized_name'].isin(common_authors)]
            post_mrs_filtered = post_mrs_df[post_mrs_df['anonymized_name'].isin(common_authors)]
            
            # Agrupar por semana
            pre_mrs = pre_mrs_filtered.groupby('week')['count'].sum().values
            post_mrs = post_mrs_filtered.groupby('week')['count'].sum().values
            
            if len(pre_mrs) > 0 and len(post_mrs) > 0:
                results['mrFrequency'] = perform_mann_whitney(
                    pre_mrs, 
                    post_mrs, 
                    "MR/PR Creation Frequency",
                    common_contributors=list(common_authors)
                )
                print(f"   ✓ MR/PR Frequency: p-value={results['mrFrequency']['pValue']:.4f}")
        else:
            print("   ⚠️ Nenhum autor comum encontrado para MR/PR Frequency")
    
    # 3. Teste para Merge Time - por autor
    if not mrs_df.empty and 'duration_hours' in mrs_df.columns and 'state' in mrs_df.columns:
        merged_mrs = mrs_df[mrs_df['state'] == 'merged'].copy()
        merged_mrs['duration_hours'] = pd.to_numeric(merged_mrs['duration_hours'], errors='coerce')
        merged_mrs = merged_mrs.dropna(subset=['duration_hours', 'anonymized_name'])
        
        if not merged_mrs.empty:
            # Separar em pre e post
            pre_merge_df = merged_mrs[merged_mrs['created_at'] < reference_date]
            post_merge_df = merged_mrs[merged_mrs['created_at'] >= reference_date]
            
            # Encontrar autores comuns
            pre_merge_authors = set(pre_merge_df['anonymized_name'].unique())
            post_merge_authors = set(post_merge_df['anonymized_name'].unique())
            common_merge_authors = pre_merge_authors & post_merge_authors
            
            if len(common_merge_authors) > 0:
                print(f"   ℹ️ Merge Time: {len(common_merge_authors)} autores comuns em ambos os períodos")
                
                # Filtrar apenas autores comuns
                pre_merge_time = pre_merge_df[pre_merge_df['anonymized_name'].isin(common_merge_authors)]['duration_hours'].values
                post_merge_time = post_merge_df[post_merge_df['anonymized_name'].isin(common_merge_authors)]['duration_hours'].values
                
                if len(pre_merge_time) > 0 and len(post_merge_time) > 0:
                    results['mergeTime'] = perform_mann_whitney(
                        pre_merge_time, 
                        post_merge_time, 
                        "Merge Time (hours)",
                        common_contributors=list(common_merge_authors)
                    )
                    print(f"   ✓ Merge Time: p-value={results['mergeTime']['pValue']:.4f}")
            else:
                print("   ⚠️ Nenhum autor comum encontrado para Merge Time")
    
    # 4. Teste para Pipeline Time Avg
    if not pipelines_df.empty:
        if 'created_at' in pipelines_df.columns and 'updated_at' in pipelines_df.columns:
            pipelines_df['created_at'] = pd.to_datetime(pipelines_df['created_at'], errors='coerce')
            pipelines_df['updated_at'] = pd.to_datetime(pipelines_df['updated_at'], errors='coerce')
            pipelines_df = pipelines_df.dropna(subset=['created_at', 'updated_at'])
            
            # Calcular duração em minutos
            pipelines_df['duration_minutes'] = (pipelines_df['updated_at'] - pipelines_df['created_at']).dt.total_seconds() / 60
            pipelines_df = pipelines_df[pipelines_df['duration_minutes'] > 0]
            
            if not pipelines_df.empty:
                # Agrupar por semana e calcular tempo médio
                pipelines_df['week'] = pipelines_df['created_at'].dt.to_period('W')
                pipeline_time_per_week = pipelines_df.groupby('week')['duration_minutes'].mean().reset_index()
                
                # Separar em pre e post
                pipeline_time_per_week['timestamp'] = pipeline_time_per_week['week'].apply(lambda x: x.to_timestamp())
                pre_pipeline_time = pipeline_time_per_week[pipeline_time_per_week['timestamp'] < reference_date]['duration_minutes'].values
                post_pipeline_time = pipeline_time_per_week[pipeline_time_per_week['timestamp'] >= reference_date]['duration_minutes'].values
                
                if len(pre_pipeline_time) > 0 and len(post_pipeline_time) > 0:
                    results['pipelineTimeAvg'] = perform_mann_whitney(
                        pre_pipeline_time, 
                        post_pipeline_time, 
                        "Pipeline Time Avg (minutes)"
                    )
                    print(f"   ✓ Pipeline Time Avg: p-value={results['pipelineTimeAvg']['pValue']:.4f}")
    
    print(f"   ✓ {len(results)} testes estatísticos realizados (Common Persons Only)")
    return results


def perform_mann_whitney_tests(commits_df, mrs_df, pipelines_df, reference_date="2024-10-08"):
    """
    DEPRECATED: Use perform_mann_whitney_tests_with_common_persons_only instead
    Mantido para compatibilidade com código existente
    """
    return {
        # "fullWorkForce": perform_mann_whitney_tests_with_full_workforce(commits_df, mrs_df, pipelines_df, reference_date),
        # "commonPersonsOnly":    perform_mann_whitney_tests_with_common_persons_only(commits_df, mrs_df, pipelines_df, reference_date)
        }


def perform_mann_whitney(pre_group, post_group, metric_name, common_contributors=None, all_contributors_pre=None, all_contributors_post=None):
    """
    Realiza teste Mann-Whitney U e calcula effect size
    """
    try:
        statistic, p_value = stats.mannwhitneyu(
            pre_group, 
            post_group, 
            alternative='two-sided'
        )
        
        # Calcular effect size (r = Z / sqrt(N))
        n1, n2 = len(pre_group), len(post_group)
        z_score = stats.norm.ppf(1 - p_value/2) if p_value > 0 else 0
        effect_size = abs(z_score) / np.sqrt(n1 + n2) if (n1 + n2) > 0 else 0
        
        # Calcular medianas dos grupos
        median_pre = float(np.median(pre_group))
        median_post = float(np.median(post_group))
        
        # Interpretação do effect size (Cohen's conventions)
        if effect_size < 0.1:
            size_interpretation = "negligible"
        elif effect_size < 0.3:
            size_interpretation = "small"
        elif effect_size < 0.5:
            size_interpretation = "medium"
        else:
            size_interpretation = "large"
        
        result = {
            "metric": metric_name,
            "statistic": float(statistic),
            "pValue": float(p_value),
            "significant": bool(p_value < 0.05),  # Converter explicitamente para bool nativo
            "effectSize": float(effect_size),
            "effectSizeInterpretation": size_interpretation,
            "n1": int(n1),
            "n2": int(n2),
            "medianPre": median_pre,
            "medianPost": median_post,
            "percentageChange": round(((median_post - median_pre) / median_pre * 100) if median_pre != 0 else 0, 2)
        }
        
        # Adicionar informações de contribuidores conforme disponível
        if common_contributors is not None:
            result["commonContributors"] = common_contributors
        if all_contributors_pre is not None:
            result["allContributorsPre"] = all_contributors_pre
        if all_contributors_post is not None:
            result["allContributorsPost"] = all_contributors_post
        
        return result
    except Exception as e:
        print(f"   ⚠️ Erro ao calcular Mann-Whitney para {metric_name}: {e}")
        return {
            "metric": metric_name,
            "error": str(e)
        }

def calculate_mr_code_churn(mrs_df):
    """
    Calcular code churn para MRs usando múltiplos fatores
    
    Fórmula de Code Churn para MRs:
    MR_Churn = α × (lines_added + lines_deleted) + β × files_changed + γ × √(total_changes)
    
    Onde:
    - α = 1.0 (peso para mudanças de linhas)
    - β = 5.0 (peso para arquivos modificados - maior impacto)
    - γ = 2.0 (peso para raiz quadrada do total de mudanças - normalização)
    - total_changes = lines_added + lines_deleted + files_changed
    
    LaTeX Formula:
    MR\_Churn = 1.0 \cdot (lines\_added + lines\_deleted) + 5.0 \cdot files\_changed + 2.0 \cdot \sqrt{lines\_added + lines\_deleted + files\_changed}
    
    Returns:
        Series: Valores de churn calculados para cada MR
    """
    # Coeficientes da fórmula
    ALPHA = 1.0   # Peso para mudanças de linhas
    BETA = 5.0    # Peso para arquivos modificados
    GAMMA = 2.0   # Peso para raiz quadrada do total
    
    # Garantir que as colunas necessárias existam e sejam numéricas
    mrs_df['lines_added'] = pd.to_numeric(mrs_df.get('lines_added', 0), errors='coerce').fillna(0)
    mrs_df['lines_deleted'] = pd.to_numeric(mrs_df.get('lines_deleted', 0), errors='coerce').fillna(0)
    mrs_df['files_changed'] = pd.to_numeric(mrs_df.get('files_changed', 0), errors='coerce').fillna(0)
    
    # Calcular componentes da fórmula
    line_changes = mrs_df['lines_added'] + mrs_df['lines_deleted']
    file_changes = mrs_df['files_changed']
    total_changes = line_changes + file_changes
    
    # Aplicar a fórmula de MR Code Churn
    # MR_Churn = α × (lines_added + lines_deleted) + β × files_changed + γ × √(total_changes)
    mr_churn = (
        ALPHA * line_changes +
        BETA * file_changes +
        GAMMA * np.sqrt(np.maximum(total_changes, 0))  # Evitar raiz de números negativos
    )
    
    return mr_churn

def main():
    args = parse_args()
    
    print("="*60)
    print("📊 PROCESSADOR DE DEVEX METRICS - GitLab")
    print("="*60)
    
    # Determinar modo de processamento
    if args.months and args.years:
        # Modo: múltiplos meses e anos
        months = [m.strip() for m in args.months.split(',')]
        years = [y.strip() for y in args.years.split(',')]
        
        print(f"\n🔄 Modo: Processamento consolidado")
        print(f"   Meses: {', '.join(months)}")
        print(f"   Anos: {', '.join(years)}")
        print(f"   Total de combinações: {len(months) * len(years)}\n")
        
        # Coletar todos os DataFrames
        all_mrs_dfs = []
        all_pipelines_dfs = []
        all_commits_dfs = []
        
        for year in years:
            for month in months:
                mrs_file, pipelines_file, commits_file = auto_find_files(args.base_dir, month, year)
                mrs_df, pipelines_df, commits_df = load_data(mrs_file, pipelines_file, commits_file)
                
                if not mrs_df.empty:
                    all_mrs_dfs.append(mrs_df)
                if not pipelines_df.empty:
                    all_pipelines_dfs.append(pipelines_df)
                if not commits_df.empty:
                    all_commits_dfs.append(commits_df)
        
        # Consolidar todos os DataFrames
        print("\n🔗 Consolidando dados de todos os períodos...")
        mrs_df = pd.concat(all_mrs_dfs, ignore_index=True) if all_mrs_dfs else pd.DataFrame()
        pipelines_df = pd.concat(all_pipelines_dfs, ignore_index=True) if all_pipelines_dfs else pd.DataFrame()
        commits_df = pd.concat(all_commits_dfs, ignore_index=True) if all_commits_dfs else pd.DataFrame()
        
        print(f"   ✓ MRs consolidados: {len(mrs_df)} registros")
        print(f"   ✓ Pipelines consolidados: {len(pipelines_df)} registros")
        print(f"   ✓ Commits consolidados: {len(commits_df)} registros")
        
        if not args.output:
            args.output = f'consolidated_metrics_{years[0]}-{years[-1]}.json'
    
    elif args.month and args.year:
        # Modo: mês e ano únicos
        mrs_file, pipelines_file, commits_file = auto_find_files(args.base_dir, args.month, args.year)
        mrs_df, pipelines_df, commits_df = load_data(mrs_file, pipelines_file, commits_file)
        
        if not args.output:
            args.output = f'devex_metrics_{args.month}_{args.year}.json'
    
    else:
        # Modo: arquivos específicos
        mrs_file = args.mrs
        pipelines_file = args.pipelines
        commits_file = args.commits
        mrs_df, pipelines_df, commits_df = load_data(mrs_file, pipelines_file, commits_file)
        
        if not args.output:
            args.output = 'devex_metrics.json'
    
    # Processar métricas
    commit_data = process_commit_metrics(commits_df, args.period)
    cicd_data = process_cicd_metrics(pipelines_df, args.period)
    pr_data = process_pr_metrics(mrs_df, args.period)
    summary_stats = calculate_summary_stats(mrs_df, pipelines_df, commits_df)
    repo_breakdown = calculate_repo_breakdown(mrs_df, pipelines_df, commits_df)
    description_patterns = analyse_pr_and_commit_descriptions(mrs_df, commits_df)
    mann_whitney_results = perform_mann_whitney_tests(commits_df, mrs_df, pipelines_df)
    
    # Montar JSON final
    output = {
        'commitData': commit_data,
        'cicdData': cicd_data,
        'prData': pr_data,
        'summaryStats': summary_stats,
        'repoBreakdown': repo_breakdown,
        'descriptionPatterns': description_patterns,
        'mannWhitneyResults': mann_whitney_results
    }
    
    # Converter todos os tipos numpy/pandas para tipos nativos Python
    output = convert_to_native_types(output)
    
    # Salvar JSON
    print(f"\n💾 Salvando arquivo JSON: {args.output}")
    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print("\n" + "="*60)
    print("✅ PROCESSAMENTO CONCLUÍDO!")
    print("="*60)
    print(f"\n📄 Arquivo gerado: {args.output}")
    print(f"\n📊 Resumo Geral:")
    print(f"   • Total de commits: {summary_stats['overall']['totalCommits']}")
    print(f"   • Total de MRs: {summary_stats['overall']['totalMRs']}")
    print(f"   • Taxa de merge: {summary_stats['overall']['avgMergeRate']}%")
    print(f"   • Pipeline success: {summary_stats['overall']['avgPipelineSuccess']}%")
    print(f"   • Repositórios ativos: {summary_stats['overall']['activeRepos']}")
    
    # Mostrar estatísticas por ano
    if summary_stats['byYear']:
        print(f"\n📊 Resumo por Ano:")
        for year, stats in sorted(summary_stats['byYear'].items()):
            print(f"   {year}:")
            print(f"      • Commits: {stats['totalCommits']}")
            print(f"      • MRs: {stats['totalMRs']}")
            print(f"      • Taxa de merge: {stats['avgMergeRate']}%")
            print(f"      • Pipeline success: {stats['avgPipelineSuccess']}%")
            print(f"      • Cycle time: {stats['avgCycleTime']}h")
    
    print(f"\n📈 Dados Temporais:")
    print(f"   • Períodos com dados: {len(commit_data)} (commits), {len(cicd_data)-1} (CI/CD), {len(pr_data)} (MRs)")
    print(f"   • Breakdown de repos: {len(repo_breakdown)}")
    print()

if __name__ == '__main__':
    main()