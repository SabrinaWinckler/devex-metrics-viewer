#!/usr/bin/env python3
"""
Script para listar repositórios ativos do GitLab nos últimos 30 dias
Filtrando apenas projetos do grupo GROUP
"""

import gitlab
from datetime import datetime, timedelta
import pytz
from dotenv import load_dotenv
import os
import csv
from collections import defaultdict
import time

# Carregando variáveis de ambiente do arquivo config.env
load_dotenv('config.env')
TOKEN = os.getenv('TOKEN')

# Configuração do grupo
GROUP_PATH = os.getenv('GROUP_PATH')

# Conectar ao GitLab
gl = gitlab.Gitlab('https://gitlab.com', private_token=TOKEN)
gl.auth()

# Teste de autenticação
user = gl.user
print(f"✓ Logado como: {user.username}")
print(f"{'='*60}")

# Calcular data de 30 dias atrás
thirty_days_ago = datetime.now(pytz.UTC) - timedelta(days=30)
thirty_days_ago_str = thirty_days_ago.strftime('%Y-%m-%dT%H:%M:%S.000Z')

print(f"\n🔍 Buscando repositórios do grupo: {GROUP_PATH}")
print(f"   Com atividade desde: {thirty_days_ago.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"{'='*60}\n")

try:
    print(f"Conectando ao grupo {GROUP_PATH}...")
    group = gl.groups.get(GROUP_PATH)
    print(f"✓ Grupo encontrado: {group.name}")
    print(f"   ID: {group.id}")
    print(f"   Descrição: {group.description if group.description else 'N/A'}")
    print(f"   URL: {group.web_url}\n")
    
    print("Buscando projetos ativos do grupo...")
    print("⏳ Isso pode levar alguns minutos dependendo da quantidade de projetos...\n")
    
    # Buscar projetos do grupo com atividade recente
    # include_subgroups=True para incluir subgrupos
    active_projects = []
    page = 1
    per_page = 50
    
    while True:
        try:
            print(f"Carregando página {page}...", end=" ", flush=True)
            
            # Buscar projetos do grupo
            projects_page = group.projects.list(
                include_subgroups=True,  # Incluir subgrupos
                archived=False,
                order_by='last_activity_at',
                sort='desc',
                page=page,
                per_page=per_page,
                get_all=False
            )
            
            if not projects_page:
                print("✓ Fim da paginação")
                break
            
            # Filtrar por data de atividade
            filtered_projects = []
            for proj in projects_page:
                last_activity = datetime.fromisoformat(
                    proj.last_activity_at.replace('Z', '+00:00')
                )
                if last_activity >= thirty_days_ago:
                    filtered_projects.append(proj)
            
            active_projects.extend(filtered_projects)
            print(f"✓ {len(filtered_projects)}/{len(projects_page)} projetos com atividade recente")
            
            # Se retornou menos que per_page, acabaram os resultados
            if len(projects_page) < per_page:
                break
            
            page += 1
            
            # Pequena pausa entre requests para evitar rate limiting
            time.sleep(0.5)
            
        except gitlab.exceptions.GitlabGetError as e:
            print(f"\n⚠️ Erro ao buscar página {page}: {e}")
            break
        except Exception as e:
            print(f"\n⚠️ Erro inesperado na página {page}: {e}")
            break
    
    print(f"\n✓ Total encontrado: {len(active_projects)} repositórios ativos no grupo {GROUP_PATH}\n")
    print(f"{'='*60}")
    print(f"REPOSITÓRIOS ATIVOS - GRUPO {GROUP_PATH.upper()}")
    print(f"{'='*60}\n")
    
    # Estrutura para armazenar dados
    repos_data = []
    
    for idx, project in enumerate(active_projects, 1):
        try:
            # Parsear data da última atividade
            last_activity = datetime.fromisoformat(
                project.last_activity_at.replace('Z', '+00:00')
            )
            days_since_activity = (datetime.now(pytz.UTC) - last_activity).days
            
            # Informações básicas do projeto
            print(f"{idx}. {project.name_with_namespace}")
            print(f"   ID: {project.id}")
            print(f"   Path: {project.path_with_namespace}")
            print(f"   Última atividade: {last_activity.strftime('%Y-%m-%d %H:%M:%S')} ({days_since_activity} dias atrás)")
            print(f"   Visibilidade: {project.visibility}")
            print(f"   Stars: {project.star_count}")
            print(f"   Forks: {project.forks_count}")
            print(f"   URL: {project.web_url}")
            
            # Obter estatísticas adicionais (opcional)
            commit_count = 0
            mr_count = 0
            
            try:
                # Contar commits recentes (limitar para evitar timeout)
                commits = project.commits.list(
                    since=thirty_days_ago_str, 
                    get_all=False, 
                    per_page=100
                )
                commit_count = len(commits)
                
                # Contar MRs recentes (limitar para evitar timeout)
                mrs = project.mergerequests.list(
                    updated_after=thirty_days_ago_str,
                    get_all=False,
                    per_page=100
                )
                mr_count = len(mrs)
                
                print(f"   Commits (últimos 30 dias): {commit_count}")
                print(f"   Merge Requests (últimos 30 dias): {mr_count}")
                
            except gitlab.exceptions.GitlabGetError as e:
                print(f"   ⚠️ Sem acesso às estatísticas (projeto privado ou sem permissão)")
            except Exception as e:
                print(f"   ⚠️ Erro ao obter estatísticas: {str(e)[:50]}")
            
            print()
            
            # Armazenar dados para CSV
            repos_data.append({
                'id': project.id,
                'name': project.name,
                'full_path': project.path_with_namespace,
                'namespace': project.namespace.get('full_path', '') if hasattr(project, 'namespace') else '',
                'visibility': project.visibility,
                'last_activity_at': last_activity.strftime('%Y-%m-%d %H:%M:%S'),
                'days_since_activity': days_since_activity,
                'stars': project.star_count,
                'forks': project.forks_count,
                'commits_last_30_days': commit_count,
                'mrs_last_30_days': mr_count,
                'web_url': project.web_url,
                'created_at': project.created_at,
                'description': project.description if project.description else ''
            })
            
            # Pequena pausa para evitar rate limiting
            if idx % 10 == 0:
                time.sleep(1)
                
        except Exception as e:
            print(f"   ⚠️ Erro ao processar projeto: {e}")
            continue
    
    # Salvar em CSV
    if repos_data:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        csv_filename = f'gitlab_active_repos-br_{timestamp}.csv'
        
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = [
                'id', 'name', 'full_path', 'namespace', 'visibility',
                'last_activity_at', 'days_since_activity', 'stars', 'forks',
                'commits_last_30_days', 'mrs_last_30_days', 'web_url',
                'created_at', 'description'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(repos_data)
        
        print(f"{'='*60}")
        print(f"✓ Dados exportados para: {csv_filename}")
        print(f"{'='*60}")
    else:
        print("⚠️ Nenhum repositório ativo encontrado nos últimos 30 dias")
    
    # Estatísticas resumidas
    if repos_data:
        print(f"\n{'='*60}")
        print("ESTATÍSTICAS RESUMIDAS")
        print(f"{'='*60}\n")
        
        total_commits = sum(r['commits_last_30_days'] for r in repos_data)
        total_mrs = sum(r['mrs_last_30_days'] for r in repos_data)
        total_stars = sum(r['stars'] for r in repos_data)
        
        print(f"Grupo: {GROUP_PATH}")
        print(f"Total de repositórios ativos: {len(repos_data)}")
        print(f"Total de commits (últimos 30 dias): {total_commits}")
        print(f"Total de Merge Requests (últimos 30 dias): {total_mrs}")
        print(f"Total de stars: {total_stars}")
        
        # Top 5 repositórios mais ativos (por commits)
        print(f"\n{'='*60}")
        print("TOP 5 REPOSITÓRIOS MAIS ATIVOS (POR COMMITS)")
        print(f"{'='*60}\n")
        
        top_repos = sorted(repos_data, key=lambda x: x['commits_last_30_days'], reverse=True)[:5]
        for idx, repo in enumerate(top_repos, 1):
            print(f"{idx}. {repo['full_path']}")
            print(f"   Commits: {repo['commits_last_30_days']} | MRs: {repo['mrs_last_30_days']}")
            print(f"   Última atividade: {repo['days_since_activity']} dias atrás")
            print()

except gitlab.exceptions.GitlabAuthenticationError:
    print("❌ Erro de autenticação!")
    print("   Verifique se o TOKEN está correto no arquivo config.env")
except gitlab.exceptions.GitlabGetError as e:
    print(f"❌ Erro ao buscar grupo ou projetos: {e}")
    print(f"   Verifique se o grupo '{GROUP_PATH}' existe e você tem permissão de acesso")
except Exception as e:
    print(f"❌ Erro inesperado: {e}")
    import traceback
    traceback.print_exc()

print(f"\n{'='*60}")
print("✅ Script concluído!")
print(f"{'='*60}\n")
