#!/usr/bin/env python3
"""
Script para extrair métricas de churn de commits e MRs do GitLab
Gera arquivos CSV com métricas detalhadas por repositório, autor e período
"""

import pandas as pd
import os
import glob
import argparse
from datetime import datetime
import re

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Extrair métricas de churn do GitLab',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  # Processar todos os arquivos normalized
  python extract_churn_metrics.py

  # Gerar saídas individuais (uma linha por commit / MR)
  python extract_churn_metrics.py --individual

  # Especificar diretório de entrada e arquivos de saída
  python extract_churn_metrics.py --input-dir ./normalized \
      --commit-output ../consolidated/churn_results/commit_churn_gitlab.csv \
      --mr-output ../consolidated/churn_results/mr_churn_gitlab.csv --individual
        """
    )
    
    parser.add_argument('--input-dir', type=str, default='./normalized',
                        help='Diretório dos arquivos normalized (padrão: ./normalized)')
    parser.add_argument('--commit-output', type=str, default='commit_churn.csv',
                        help='Arquivo CSV de saída para commit churn (padrão: commit_churn.csv)')
    parser.add_argument('--mr-output', type=str, default='mr_churn.csv',
                        help='Arquivo CSV de saída para MR churn (padrão: mr_churn.csv)')
    parser.add_argument('--individual', action='store_true',
                        help='Gerar saídas individuais (uma linha por commit / MR) em vez de agregar por autor/repositorio')
    
    return parser.parse_args()

def extract_period_from_filename(filename):
    """
    Extrair período (YYYYMM) do nome do arquivo
    Exemplo: gitlab_commits_20250701_to_20250730_20251007_153111.csv -> 202507
    """
    # Padrão para extrair data inicial: YYYYMMDD
    match = re.search(r'_(\d{8})_to_', filename)
    if match:
        date_str = match.group(1)
        # Converter YYYYMMDD para YYYYMM
        return date_str[:6]
    
    return None

def get_repo_name(repo_path):
    """
    Extrair nome do repositório do caminho completo
    Exemplo: grupo/subgrupo/repo-name -> repo-name
    """
    if pd.isna(repo_path) or not repo_path:
        return 'unknown'
    
    return repo_path.split('/')[-1]

def process_commit_churn(input_dir, individual=False):
    """
    Processar arquivos de commits para extrair métricas de churn
    If individual=True, produce one output line per commit (no aggregation)
    """
    print("📊 Processando métricas de churn de commits...")
    
    # Buscar todos os arquivos de commits
    commit_files = glob.glob(os.path.join(input_dir, 'gitlab_commits_*.csv'))
    
    all_commit_data = []
    
    for file_path in commit_files:
        print(f"   📄 Processando: {os.path.basename(file_path)}")
        
        # Extrair período do nome do arquivo
        period = extract_period_from_filename(os.path.basename(file_path))
        if not period:
            print(f"   ⚠️ Não foi possível extrair período de: {file_path}")
            continue
        
        year = period[:4]
        month = period[4:]
        
        try:
            # Carregar arquivo CSV
            df = pd.read_csv(file_path)
            
            # Verificar se tem as colunas necessárias
            required_cols = ['repository', 'anonymized_name', 'lines_added', 'lines_deleted']
            if not all(col in df.columns for col in required_cols):
                print(f"   ⚠️ Colunas necessárias não encontradas em: {file_path}")
                continue
            
            # Converter colunas numéricas
            df['lines_added'] = pd.to_numeric(df['lines_added'], errors='coerce').fillna(0)
            df['lines_deleted'] = pd.to_numeric(df['lines_deleted'], errors='coerce').fillna(0)
            
            # Filtrar registros válidos
            df = df[df['anonymized_name'].notna() & (df['anonymized_name'] != 'P n/a')]
            df = df[df['repository'].notna()]
            
            if individual:
                # One row per commit
                for _, row in df.iterrows():
                    author = row['anonymized_name']
                    repo_path = row['repository']
                    repo_name = get_repo_name(repo_path)
                    repo_slug = repo_name
                    la = int(row['lines_added'])
                    ld = int(row['lines_deleted'])
                    total_churn = la + ld
                    net_change = la - ld
                    all_commit_data.append({
                        'period': period,
                        'year': year,
                        'month': month,
                        'repo_slug': repo_slug,
                        'repo_name': repo_name,
                        'author': author,
                        'commits': 1,
                        'lines_added': la,
                        'lines_removed': ld,
                        'total_churn': total_churn,
                        'net_change': net_change
                    })
            else:
                # Agrupar por repositório e autor (comportamento antigo)
                grouped = df.groupby(['repository', 'anonymized_name']).agg({
                    'lines_added': 'sum',
                    'lines_deleted': 'sum',
                    'anonymized_name': 'count'  # Contar commits
                }).rename(columns={'anonymized_name': 'commits'})
                
                # Calcular métricas de churn
                for (repo_path, author), stats in grouped.iterrows():
                    repo_name = get_repo_name(repo_path)
                    repo_slug = repo_name  # Usar nome como slug
                    
                    total_churn = stats['lines_added'] + stats['lines_deleted']
                    net_change = stats['lines_added'] - stats['lines_deleted']
                    
                    all_commit_data.append({
                        'period': period,
                        'year': year,
                        'month': month,
                        'repo_slug': repo_slug,
                        'repo_name': repo_name,
                        'author': author,
                        'commits': int(stats['commits']),
                        'lines_added': int(stats['lines_added']),
                        'lines_removed': int(stats['lines_deleted']),
                        'total_churn': int(total_churn),
                        'net_change': int(net_change)
                    })
            
        except Exception as e:
            print(f"   ⚠️ Erro ao processar {file_path}: {e}")
            continue
    
    print(f"   ✓ {len(all_commit_data)} registros de commit churn processados")
    return all_commit_data

def process_mr_churn(input_dir, individual=False):
    """
    Processar arquivos de MRs para extrair métricas de churn
    If individual=True, produce one output line per MR (no aggregation)
    """
    print("🔀 Processando métricas de churn de MRs...")
    
    # Buscar todos os arquivos de MRs
    mr_files = glob.glob(os.path.join(input_dir, 'gitlab_mrs_*.csv'))
    
    all_mr_data = []
    
    for file_path in mr_files:
        print(f"   📄 Processando: {os.path.basename(file_path)}")
        
        # Extrair período do nome do arquivo
        period = extract_period_from_filename(os.path.basename(file_path))
        if not period:
            print(f"   ⚠️ Não foi possível extrair período de: {file_path}")
            continue
        
        year = period[:4]
        month = period[4:]
        
        try:
            # Carregar arquivo CSV
            df = pd.read_csv(file_path)
            
            # Verificar se tem as colunas necessárias
            required_cols = ['repository', 'anonymized_name', 'lines_added', 'lines_deleted']
            if not all(col in df.columns for col in required_cols):
                print(f"   ⚠️ Colunas necessárias não encontradas em: {file_path}")
                continue
            
            # Converter colunas numéricas
            df['lines_added'] = pd.to_numeric(df['lines_added'], errors='coerce').fillna(0)
            df['lines_deleted'] = pd.to_numeric(df['lines_deleted'], errors='coerce').fillna(0)
            
            # Filtrar registros válidos
            df = df[df['anonymized_name'].notna() & (df['anonymized_name'] != 'P n/a')]
            df = df[df['repository'].notna()]
            
            if individual:
                # One row per MR
                for _, row in df.iterrows():
                    author = row['anonymized_name']
                    repo_path = row['repository']
                    repo_name = get_repo_name(repo_path)
                    repo_slug = repo_name
                    la = int(row['lines_added'])
                    ld = int(row['lines_deleted'])
                    total_churn = la + ld
                    net_change = la - ld
                    avg_churn_per_pr = total_churn  # single PR
                    all_mr_data.append({
                        'period': period,
                        'year': year,
                        'month': month,
                        'repo_slug': repo_slug,
                        'repo_name': repo_name,
                        'author': author,
                        'prs': 1,
                        'lines_added': la,
                        'lines_removed': ld,
                        'total_churn': total_churn,
                        'net_change': net_change,
                        'avg_churn_per_pr': float(avg_churn_per_pr)
                    })
            else:
                # Agrupar por repositório e autor (comportamento antigo)
                grouped = df.groupby(['repository', 'anonymized_name']).agg({
                    'lines_added': 'sum',
                    'lines_deleted': 'sum',
                    'anonymized_name': 'count'  # Contar MRs
                }).rename(columns={'anonymized_name': 'prs'})
                
                # Calcular métricas de churn
                for (repo_path, author), stats in grouped.iterrows():
                    repo_name = get_repo_name(repo_path)
                    repo_slug = repo_name  # Usar nome como slug
                    
                    total_churn = stats['lines_added'] + stats['lines_deleted']
                    net_change = stats['lines_added'] - stats['lines_deleted']
                    avg_churn_per_pr = total_churn / stats['prs'] if stats['prs'] > 0 else 0
                    
                    all_mr_data.append({
                        'period': period,
                        'year': year,
                        'month': month,
                        'repo_slug': repo_slug,
                        'repo_name': repo_name,
                        'author': author,
                        'prs': int(stats['prs']),
                        'lines_added': int(stats['lines_added']),
                        'lines_removed': int(stats['lines_deleted']),
                        'total_churn': int(total_churn),
                        'net_change': int(net_change),
                        'avg_churn_per_pr': round(avg_churn_per_pr, 1)
                    })
            
        except Exception as e:
            print(f"   ⚠️ Erro ao processar {file_path}: {e}")
            continue
    
    print(f"   ✓ {len(all_mr_data)} registros de MR churn processados")
    return all_mr_data

def save_churn_data(data, output_file, data_type):
    """
    Salvar dados de churn em arquivo CSV
    """
    if not data:
        print(f"   ⚠️ Nenhum dado de {data_type} para salvar")
        return
    
    df = pd.DataFrame(data)
    
    # Ordenar por período, repositório e autor
    sort_cols = ['period', 'repo_name', 'author']
    existing_sort = [c for c in sort_cols if c in df.columns]
    if existing_sort:
        df = df.sort_values(existing_sort)
    
    # Salvar CSV
    df.to_csv(output_file, index=False)
    print(f"   ✓ {len(df)} registros de {data_type} salvos em: {output_file}")

def main():
    args = parse_args()
    
    print("="*60)
    print("📊 EXTRATOR DE MÉTRICAS DE CHURN - GitLab")
    print("="*60)
    
    # Verificar se diretório de entrada existe
    if not os.path.exists(args.input_dir):
        print(f"❌ Diretório não encontrado: {args.input_dir}")
        return
    
    print(f"\n📂 Diretório de entrada: {args.input_dir}")
    print(f"📄 Arquivo de saída (commits): {args.commit_output}")
    print(f"📄 Arquivo de saída (MRs): {args.mr_output}")
    print(f"📄 Modo individual (sem agregação): {args.individual}")
    
    # Processar métricas de churn de commits
    commit_data = process_commit_churn(args.input_dir, individual=args.individual)
    save_churn_data(commit_data, args.commit_output, 'commit churn')
    
    print()
    
    # Processar métricas de churn de MRs
    mr_data = process_mr_churn(args.input_dir, individual=args.individual)
    save_churn_data(mr_data, args.mr_output, 'MR churn')
    
    print("\n" + "="*60)
    print("✅ EXTRAÇÃO CONCLUÍDA!")
    print("="*60)
    
    # Mostrar estatísticas finais
    if commit_data:
        print(f"\n📊 Estatísticas de Commit Churn:")
        commit_df = pd.DataFrame(commit_data)
        print(f"   • Total de registros: {len(commit_df)}")
        print(f"   • Períodos únicos: {commit_df['period'].nunique()}")
        print(f"   • Repositórios únicos: {commit_df['repo_name'].nunique()}")
        print(f"   • Autores únicos: {commit_df['author'].nunique()}")
        print(f"   • Total de commits: {commit_df['commits'].sum()}")
        print(f"   • Total churn: {commit_df['total_churn'].sum():,}")
    
    if mr_data:
        print(f"\n🔀 Estatísticas de MR Churn:")
        mr_df = pd.DataFrame(mr_data)
        print(f"   • Total de registros: {len(mr_df)}")
        print(f"   • Períodos únicos: {mr_df['period'].nunique()}")
        print(f"   • Repositórios únicos: {mr_df['repo_name'].nunique()}")
        print(f"   • Autores únicos: {mr_df['author'].nunique()}")
        print(f"   • Total de MRs: {mr_df['prs'].sum()}")
        print(f"   • Total churn: {mr_df['total_churn'].sum():,}")
    
    print()

if __name__ == '__main__':
    main()