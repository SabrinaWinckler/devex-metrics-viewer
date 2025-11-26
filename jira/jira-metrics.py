#!/usr/bin/env python3
"""
Script para processar métricas do Jira
Gera métricas de tickets (created, resolved, in progress, resolution time) e carga cognitiva por pessoa
"""

import pandas as pd
import json
from datetime import datetime, timedelta
import numpy as np
import os
import argparse
from collections import defaultdict

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Processar métricas do Jira',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  # Processar arquivo normalizado padrão
  python jira-metrics.py
  
  # Especificar arquivo de entrada
  python jira-metrics.py --input normalized/Jira.csv
  
  # Especificar arquivo de saída
  python jira-metrics.py --output jira_metrics.json
  
  # Agregação semanal (padrão) ou mensal
  python jira-metrics.py --period monthly
        """
    )
    
    parser.add_argument('--input', type=str, default='normalized/Jira.csv',
                        help='Arquivo CSV de entrada (padrão: normalized/Jira.csv)')
    parser.add_argument('--output', type=str, default='jira_metrics.json',
                        help='Arquivo JSON de saída (padrão: jira_metrics.json)')
    parser.add_argument('--period', type=str, choices=['weekly', 'monthly'], default='weekly',
                        help='Período de agregação: weekly ou monthly (padrão: weekly)')
    
    return parser.parse_args()


def get_period_label(date, period='weekly'):
    """Gerar label do período com data real"""
    if pd.isna(date):
        return None
    
    if period == 'weekly':
        # Retorna a data da segunda-feira da semana
        monday = date - timedelta(days=date.weekday())
        return monday.strftime('%Y-%m-%d')
    else:  # monthly
        # Retorna o primeiro dia do mês
        return date.strftime('%Y-%m-01')


def calculate_resolution_time(created, resolved):
    """Calcular tempo de resolução em horas"""
    if pd.isna(created) or pd.isna(resolved):
        return None
    
    try:
        created_dt = pd.to_datetime(created)
        resolved_dt = pd.to_datetime(resolved)
        diff = (resolved_dt - created_dt).total_seconds() / 3600  # converter para horas
        return diff if diff >= 0 else None
    except:
        return None


def process_jira_data(df, period='weekly'):
    """
    Processar métricas de tickets do Jira por período
    """
    print("\n📊 Processando métricas de tickets do Jira...")
    
    if df.empty:
        print("   ⚠️ DataFrame vazio")
        return []
    
    # Converter datas
    df['Created'] = pd.to_datetime(df['Created'], errors='coerce')
    df['Resolved'] = pd.to_datetime(df['Resolved'], errors='coerce')
    
    # Remover linhas sem data de criação
    df = df.dropna(subset=['Created'])
    
    # Adicionar coluna de período
    df['period'] = df['Created'].apply(lambda x: get_period_label(x, period))
    df = df.dropna(subset=['period'])
    
    # Calcular tempo de resolução
    df['resolution_time'] = df.apply(lambda row: calculate_resolution_time(row['Created'], row['Resolved']), axis=1)
    
    # Agregar por período
    jira_data = []
    
    for period_label in sorted(df['period'].unique()):
        period_df = df[df['period'] == period_label]
        
        # Tickets criados no período
        created = len(period_df)
        
        # Tickets resolvidos no período (usar a data de resolução)
        resolved_in_period = 0
        if 'Resolved' in df.columns:
            resolved_df = df[df['Resolved'].notna()]
            resolved_df['resolved_period'] = resolved_df['Resolved'].apply(lambda x: get_period_label(x, period))
            resolved_in_period = len(resolved_df[resolved_df['resolved_period'] == period_label])
        
        # Tickets em progresso (criados mas não resolvidos ainda)
        in_progress = len(period_df[period_df['Resolved'].isna()])
        
        # Calcular métricas de tempo de resolução
        resolution_times = period_df['resolution_time'].dropna()
        avg_resolution_time = round(resolution_times.mean(), 1) if len(resolution_times) > 0 else 0
        p95_resolution_time = round(resolution_times.quantile(0.95), 1) if len(resolution_times) > 0 else 0
        
        jira_data.append({
            'date': period_label,
            'created': created,
            'resolved': resolved_in_period,
            'inProgress': in_progress,
            'avgResolutionTime': avg_resolution_time,
            'p95ResolutionTime': p95_resolution_time, 
            'persons': period_df['anonymized_assignee'].dropna().unique().tolist()
        })
    
    print(f"   ✓ {len(jira_data)} períodos processados")
    return jira_data


def calculate_cognitive_load(df):
    """
    Calcular carga cognitiva por pessoa (assignee)
    """
    print("\n🧠 Calculando carga cognitiva por pessoa...")
    
    if df.empty or 'anonymized_assignee' not in df.columns:
        print("   ⚠️ Coluna anonymized_assignee não encontrada")
        return []
    
    # Filtrar apenas tickets com assignee válido
    df_assigned = df[df['anonymized_assignee'].notna() & (df['anonymized_assignee'] != '') & (df['anonymized_assignee'] != 'P n/a')]
    
    if df_assigned.empty:
        print("   ⚠️ Nenhum ticket com assignee válido")
        return []
    
    # Convert date columns to datetime
    df_assigned['Updated'] = pd.to_datetime(df_assigned['Updated'], errors='coerce')
    df_assigned['Created'] = pd.to_datetime(df_assigned['Created'], errors='coerce')
    df_assigned['Resolved'] = pd.to_datetime(df_assigned['Resolved'], errors='coerce')
    
    # Agrupar por pessoa
    cognitive_load = []
    
    for person_id in sorted(df_assigned['anonymized_assignee'].unique()):
        person_tickets = df_assigned[df_assigned['anonymized_assignee'] == person_id]
        
        # Contar tickets atribuídos (equivalente a review count)
        ticket_count = len(person_tickets)
        
        # Coletar projetos únicos (se disponível)
        projects = []
        if 'Components' in person_tickets.columns:
            projects = person_tickets['Components'].dropna().unique().tolist()
        
        #anonimizar projetos
        projects = [f"project_{i+1}" for i in range(len(projects))]
        #conte quantos cards resolvidos a pessoa tem por mes e ano (07-2024, 08-2024 e 09-2024) e (07-2025, 08-2025 e 09-2025)
        resolved_counts = defaultdict(int)
        for _, row in person_tickets.iterrows():
            if pd.notna(row['Resolved']):
                key = row['Resolved'].strftime('%m-%Y')
                resolved_counts[key] += 1
        
        # Adicionar ao resultado
        cognitive_load.append({
            'personId': person_id,
            'ticketCount': ticket_count,
            'projects': projects if projects else [],
            'hasJiraOperations': True,
            'dates_updated': [df_assigned['Updated'].dropna().dt.strftime('%Y-%m-%d').min(), df_assigned['Updated'].dropna().dt.strftime('%Y-%m-%d').max()],
            'date_created': [df_assigned['Created'].dropna().dt.strftime('%Y-%m-%d').min(), df_assigned['Created'].dropna().dt.strftime('%Y-%m-%d').max()],
            'date_resolved': resolved_counts        
            })
    
    # Ordenar por número de tickets (decrescente)
    cognitive_load.sort(key=lambda x: x['ticketCount'], reverse=True)
    
    print(f"   ✓ {len(cognitive_load)} pessoas processadas")
    return cognitive_load


def main():
    args = parse_args()
    
    print("="*60)
    print("📊 PROCESSADOR DE MÉTRICAS DO JIRA")
    print("="*60)
    
    # Verificar se o arquivo existe
    if not os.path.exists(args.input):
        print(f"\n❌ Erro: Arquivo '{args.input}' não encontrado!")
        print("   Certifique-se de que o arquivo normalizado existe.")
        return
    
    # Carregar CSV
    print(f"\n📂 Carregando: {args.input}")
    try:
        df = pd.read_csv(args.input)
        print(f"   ✓ {len(df)} registros carregados")
        print(f"   ✓ Colunas principais: {', '.join(df.columns[:10].tolist())}...")
    except Exception as e:
        print(f"   ❌ Erro ao carregar CSV: {e}")
        return
    
    # Processar métricas
    jira_data = process_jira_data(df, args.period)
    cognitive_load = calculate_cognitive_load(df)
    
    # Montar JSON final
    output = {
        'jiraData': jira_data,
        'cognitiveLoad': cognitive_load
    }
    
    # Salvar JSON
    print(f"\n💾 Salvando arquivo JSON: {args.output}")
    try:
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        print(f"   ✓ Arquivo salvo com sucesso")
    except Exception as e:
        print(f"   ❌ Erro ao salvar JSON: {e}")
        return
    
    # Resumo
    print("\n" + "="*60)
    print("✅ PROCESSAMENTO CONCLUÍDO!")
    print("="*60)
    print(f"\n📄 Arquivo gerado: {args.output}")
    print(f"\n📊 Resumo:")
    print(f"   • Períodos com dados: {len(jira_data)}")
    print(f"   • Pessoas com tickets: {len(cognitive_load)}")
    
    if jira_data:
        total_created = sum(d['created'] for d in jira_data)
        total_resolved = sum(d['resolved'] for d in jira_data)
        print(f"   • Total de tickets criados: {total_created}")
        print(f"   • Total de tickets resolvidos: {total_resolved}")
    
    if cognitive_load:
        top_person = cognitive_load[0]
        print(f"   • Pessoa com mais tickets: {top_person['personId']} ({top_person['ticketCount']} tickets)")
    
    print()


if __name__ == '__main__':
    main()