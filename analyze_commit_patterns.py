#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Análise de Padrões de Commits
Identifica padrões em mensagens de commits incluindo padrões JIRA
Gera CSV e JSON com resultados agregados
"""

import pandas as pd
import re
from datetime import datetime
import json
from collections import defaultdict

# Constantes
BITBUCKET_CSV = 'consolidated/bitbucket_commits_merged_20251024_114911.csv'
GITLAB_CSV = 'consolidated/gitlab_commits_merged_20251024_114758.csv'
OUTPUT_CSV = 'commit_patterns_analysis.csv'
OUTPUT_JSON = 'commit_patterns_analysis.json'

# Padrões regex para JIRA
JIRA_PATTERNS = [
    r'.*\[([A-Z]+)-(\d+)\]*.',  # [PROJECT-123]
    r'.*\[([A-Z]+)\]*.',         # [PROJECT]
    r'.*([A-Z]+)-(\d+)*.',       # PROJECT-123 (sem colchetes)
]

# Padrões de palavras-chave
KEYWORD_PATTERNS = {
    'fix/bug/issue': ['fix', 'bug', 'issue', 'hotfix', 'bugfix', 'correction', 'corr'],
    'doc/documentation/readme': ['doc', 'documentation', 'readme', 'docs'],
    'test/testing/ci/coverage': ['test', 'testing', 'ci', 'coverage', 'spec'],
    'refactor/cleanup/clean/restructure': ['refactor', 'cleanup', 'clean', 'restructure', 'improve'],
    'update/upgrade/bump/version': ['update', 'upgrade', 'bump', 'version', 'migrate'],
    'chore/misc/miscellaneous': ['chore', 'misc', 'miscellaneous', 'merge', 'style']
}


def check_jira_pattern(message):
    """Verifica se a mensagem contém padrão JIRA"""
    if not isinstance(message, str):
        return False
    
    for pattern in JIRA_PATTERNS:
        if re.search(pattern, message):
            return True
    return False


def classify_commit(message):
    """Classifica o commit baseado em padrões"""
    if not isinstance(message, str):
        return 'other'
    
    message_lower = message.lower()
    
    # Primeiro verifica se tem padrão JIRA - prioridade para feature/add/new/feat
    if check_jira_pattern(message):
        return 'feature/add/new/feat'
    
    # Verifica palavras-chave
    for pattern_name, keywords in KEYWORD_PATTERNS.items():
        if any(keyword in message_lower for keyword in keywords):
            return pattern_name
    
    # Se tem 'add', 'new', 'feature', 'feat', 'implement' também é feature
    feature_keywords = ['feature', 'add', 'new', 'feat', 'implement']
    if any(keyword in message_lower for keyword in feature_keywords):
        return 'feature/add/new/feat'
    
    return 'other'


def load_and_process_commits(csv_path, source_name):
    """Carrega e processa commits de um CSV"""
    print(f"\n📂 Carregando {source_name}...")
    
    try:
        df = pd.read_csv(csv_path)
        print(f"   ✓ {len(df)} commits carregados")
        
        # Adicionar coluna de fonte
        df['source'] = source_name
        
        # Garantir que as colunas necessárias existem
        required_cols = ['message', 'anonymized_name', 'date', 'total_churn', 'net_change']
        
        # Verificar colunas disponíveis
        if 'message' not in df.columns:
            print(f"   ⚠️  Coluna 'message' não encontrada. Colunas disponíveis: {df.columns.tolist()}")
            return pd.DataFrame()
        
        # Normalizar nomes de colunas (GitLab usa 'lines_deleted' e Bitbucket usa 'lines_removed')
        if 'lines_deleted' in df.columns and 'lines_removed' not in df.columns:
            df['lines_removed'] = df['lines_deleted']
        
        # Calcular total_churn e net_change se não existirem
        if 'total_churn' not in df.columns:
            if 'lines_added' in df.columns and 'lines_removed' in df.columns:
                df['total_churn'] = df['lines_added'].fillna(0) + df['lines_removed'].fillna(0)
                df['net_change'] = df['lines_added'].fillna(0) - df['lines_removed'].fillna(0)
            else:
                print(f"   ⚠️  Colunas de churn não encontradas. Colunas: {df.columns.tolist()}")
                df['total_churn'] = 0
                df['net_change'] = 0
        else:
            # Preencher valores nulos com 0
            df['total_churn'] = df['total_churn'].fillna(0)
            df['net_change'] = df['net_change'].fillna(0)
        
        # Converter data para datetime (UTC e remover timezone para evitar conflitos)
        date_col = 'date' if 'date' in df.columns else 'created_at'
        if date_col in df.columns:
            df['date'] = pd.to_datetime(df[date_col], errors='coerce', utc=True).dt.tz_localize(None)
        else:
            print(f"   ⚠️  Coluna de data não encontrada")
            df['date'] = pd.NaT
        
        # Classificar commits
        print(f"   🔍 Classificando padrões de commits...")
        df['type_pattern'] = df['message'].apply(classify_commit)
        
        # Estatísticas
        pattern_counts = df['type_pattern'].value_counts()
        print(f"   📊 Padrões identificados:")
        for pattern, count in pattern_counts.items():
            print(f"      - {pattern}: {count}")
        
        return df
        
    except FileNotFoundError:
        print(f"   ❌ Arquivo não encontrado: {csv_path}")
        return pd.DataFrame()
    except Exception as e:
        print(f"   ❌ Erro ao processar {csv_path}: {str(e)}")
        return pd.DataFrame()


def generate_csv_output(df, output_path):
    """Gera arquivo CSV com os resultados"""
    print(f"\n📝 Gerando CSV de saída...")
    
    # Selecionar e ordenar colunas
    output_df = df[['type_pattern', 'anonymized_name', 'total_churn', 'net_change', 'date', 'source']].copy()
    
    # Ordenar por data
    output_df = output_df.sort_values('date', ascending=False)
    
    # Salvar CSV
    output_df.to_csv(output_path, index=False)
    print(f"   ✓ CSV salvo em: {output_path}")
    print(f"   ✓ Total de linhas: {len(output_df)}")


def generate_json_output(df, output_path):
    """Gera arquivo JSON com agregações por tipo de padrão"""
    print(f"\n📊 Gerando JSON de saída...")
    
    result = {}
    
    # Análise de Commits separada por ano
    if not df.empty and 'date' in df.columns:
        # Converter date para datetime e adicionar coluna de ano
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df['year'] = df['date'].dt.year
        
        # Análise por ano
        commits_by_year = {}
        
        for year in sorted(df['year'].dropna().unique()):
            year_commits = df[df['year'] == year].copy()
            
            pattern_counts = {
                'fix/bug/issue': {'count': 0, 'contributors': set(), 'codeChurn': 0, 'netChange': 0, 'dates': []},
                'feature/add/new/feat': {'count': 0, 'contributors': set(), 'codeChurn': 0, 'netChange': 0, 'dates': []},
                'doc/documentation/readme': {'count': 0, 'contributors': set(), 'codeChurn': 0, 'netChange': 0, 'dates': []},
                'test/testing/ci/coverage': {'count': 0, 'contributors': set(), 'codeChurn': 0, 'netChange': 0, 'dates': []},
                'refactor/cleanup/clean/restructure': {'count': 0, 'contributors': set(), 'codeChurn': 0, 'netChange': 0, 'dates': []},
                'update/upgrade/bump/version': {'count': 0, 'contributors': set(), 'codeChurn': 0, 'netChange': 0, 'dates': []},
                'chore/misc/miscellaneous': {'count': 0, 'contributors': set(), 'codeChurn': 0, 'netChange': 0, 'dates': []},
                'other': {'count': 0, 'contributors': set(), 'codeChurn': 0, 'netChange': 0, 'dates': []}
            }
            
            # Agrupar por tipo de padrão no ano
            for pattern in year_commits['type_pattern'].unique():
                pattern_df = year_commits[year_commits['type_pattern'] == pattern].copy()
                
                if pattern in pattern_counts:
                    pattern_counts[pattern]['count'] = int(len(pattern_df))
                    pattern_counts[pattern]['codeChurn'] = int(pattern_df['total_churn'].sum())
                    pattern_counts[pattern]['netChange'] = int(pattern_df['net_change'].sum())
                    pattern_counts[pattern]['contributors'] = set(pattern_df['anonymized_name'].dropna().unique())
                    pattern_counts[pattern]['dates'] = pattern_df['date'].dt.strftime('%Y-%m-%d').tolist()
            
            # Converter sets para listas e contar contributors
            year_result = {}
            for pattern, data in pattern_counts.items():
                year_result[pattern] = {
                    'totalCommits': data['count'],
                    'totalChurn': data['codeChurn'],
                    'totalNet': data['netChange'],
                    'totalContributors': len(data['contributors']),
                    'year': int(year)
                }
            
            commits_by_year[int(year)] = year_result
        
        result['byYear'] = commits_by_year
    
    # Adicionar totais gerais
    result['TOTAL'] = {
        'totalCommits': int(len(df)),
        'totalChurn': int(df['total_churn'].sum()),
        'totalNet': int(df['net_change'].sum()),
        'totalContributors': int(df['anonymized_name'].nunique())
    }
    
    # Salvar JSON
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    
    print(f"   ✓ JSON salvo em: {output_path}")
    print(f"\n📈 Resumo por ano:")
    if 'byYear' in result:
        for year, patterns in sorted(result['byYear'].items()):
            print(f"\n   Ano {year}:")
            for pattern, stats in sorted(patterns.items()):
                if stats['totalCommits'] > 0:
                    print(f"      {pattern}: {stats['totalCommits']} commits")


def main():
    """Função principal"""
    print("=" * 80)
    print("🚀 ANÁLISE DE PADRÕES DE COMMITS")
    print("=" * 80)
    
    # Carregar dados do Bitbucket
    bitbucket_df = load_and_process_commits(BITBUCKET_CSV, 'bitbucket')
    
    # Carregar dados do GitLab
    gitlab_df = load_and_process_commits(GITLAB_CSV, 'gitlab')
    
    # Combinar dataframes
    if not bitbucket_df.empty or not gitlab_df.empty:
        all_commits = pd.concat([bitbucket_df, gitlab_df], ignore_index=True)
        print(f"\n✅ Total de commits combinados: {len(all_commits)}")
        
        # Remover commits sem mensagem
        all_commits = all_commits[all_commits['message'].notna()]
        print(f"   ✓ Commits com mensagem válida: {len(all_commits)}")
        
        # Gerar CSV
        generate_csv_output(all_commits, OUTPUT_CSV)
        
        # Gerar JSON
        generate_json_output(all_commits, OUTPUT_JSON)
        
        print("\n" + "=" * 80)
        print("✅ ANÁLISE CONCLUÍDA COM SUCESSO!")
        print("=" * 80)
        print(f"\n📁 Arquivos gerados:")
        print(f"   - {OUTPUT_CSV}")
        print(f"   - {OUTPUT_JSON}")
        
    else:
        print("\n❌ Nenhum dado foi carregado. Verifique os arquivos CSV.")


if __name__ == '__main__':
    main()
