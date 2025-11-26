#!/usr/bin/env python3
"""
Script para listar repositórios ativos nos últimos 30 dias do Bitbucket
Equivalente ao script do GitLab para análise de repositórios ativos
"""

import os
import sys
import csv
from datetime import datetime, timedelta
import pytz
import requests
from requests.auth import HTTPBasicAuth

# Configurações
BITBUCKET_WORKSPACE = os.getenv("BITBUCKET_WORKSPACE")
BITBUCKET_USERNAME = os.getenv("BITBUCKET_USERNAME", "")
BITBUCKET_API_TOKEN = os.getenv("BITBUCKET_API_TOKEN", "")
BITBUCKET_API_URL = "https://api.bitbucket.org/2.0"


def test_authentication(session, workspace):
    """Teste de autenticação - equivalente ao gl.auth()"""
    try:
        # Tentar buscar informações do usuário
        response = session.get(f"{BITBUCKET_API_URL}/user")
        response.raise_for_status()
        user_data = response.json()
        
        print(f"✓ Logado como: {user_data.get('username', user_data.get('display_name'))}")
        print(f"{'='*60}")
        
        return user_data
    except requests.exceptions.RequestException as e:
        print(f"❌ Erro de autenticação: {e}")
        sys.exit(1)


def get_repository_last_activity(session, workspace, repo_slug):
    """Obter a data da última atividade do repositório (último commit)"""
    try:
        url = f"{BITBUCKET_API_URL}/repositories/{workspace}/{repo_slug}/commits"
        response = session.get(url, params={'pagelen': 1})
        response.raise_for_status()
        
        data = response.json()
        commits = data.get('values', [])
        
        if commits:
            commit_date_str = commits[0].get('date', '')
            if commit_date_str:
                return datetime.fromisoformat(commit_date_str.replace('Z', '+00:00'))
        
        return None
    except Exception as e:
        print(f"   ⚠️  Erro ao buscar atividade: {e}")
        return None


def get_active_repositories(session, workspace, thirty_days_ago):
    """
    Buscar repositórios com atividade nos últimos 30 dias
    Equivalente ao group.projects.list() do GitLab
    """
    print(f"\n🔍 Buscando repositórios do workspace: {workspace}")
    print(f"   Com atividade desde: {thirty_days_ago.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")
    
    print("Buscando repositórios do workspace...")
    print("⏳ Isso pode levar alguns minutos dependendo da quantidade de repositórios...\n")
    
    active_repositories = []
    page = 1
    per_page = 50
    should_continue = True
    
    while should_continue:
        try:
            print(f"Carregando página {page}...", end=" ", flush=True)
            
            # Buscar repositórios do workspace
            url = f"{BITBUCKET_API_URL}/repositories/{workspace}"
            params = {
                'page': page,
                'pagelen': per_page,
                'sort': '-updated_on'  # Ordenar por última atualização (DESC)
            }
            
            response = session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            repos_page = data.get('values', [])
            
            if not repos_page:
                print("✓ Fim da paginação")
                break
            
            print(f"✓ {len(repos_page)} repositórios encontrados")
            
            # Filtrar por data de atividade
            filtered_repos = []
            repos_without_activity_count = 0
            
            for repo in repos_page:
                repo_slug = repo.get('slug', '')
                repo_name = repo.get('name', repo_slug)
                
                print(f"   Verificando: {repo_name}...", end=" ", flush=True)
                
                # Buscar última atividade (último commit)
                last_activity = get_repository_last_activity(session, workspace, repo_slug)
                
                # Comparar datetimes - ambos devem ter timezone
                if last_activity and last_activity >= thirty_days_ago:
                    # Adicionar informação de última atividade ao repositório
                    repo['last_activity_at'] = last_activity.isoformat()
                    filtered_repos.append(repo)
                    print("✓ ATIVO")
                else:
                    repos_without_activity_count += 1
                    print("✗ Sem atividade recente")
                    
                    # OTIMIZAÇÃO: Se encontrarmos múltiplos repos sem atividade seguidos,
                    # e como estão ordenados por updated_on DESC, os próximos também não terão
                    # Parar após encontrar 5 repositórios consecutivos sem atividade
                    if repos_without_activity_count >= 5:
                        print(f"\n   ⚡ Otimização: Encontrados {repos_without_activity_count} repositórios consecutivos sem atividade.")
                        print(f"   Como a lista está ordenada por última atualização (DESC),")
                        print(f"   os próximos repositórios também não terão atividade recente.")
                        print(f"   Parando a busca...")
                        should_continue = False
                        break
            
            active_repositories.extend(filtered_repos)
            
            # Se não deve continuar, sair do loop
            if not should_continue:
                break
            
            # Verificar se há próxima página
            if 'next' not in data:
                print("\n✓ Todas as páginas processadas")
                break
            
            page += 1
            
        except requests.exceptions.RequestException as e:
            print(f"\n❌ Erro ao buscar repositórios: {e}")
            break
        except KeyboardInterrupt:
            print("\n\n⚠️  Interrompido pelo usuário")
            break
    
    return active_repositories


def save_to_csv(repositories, filename):
    """Salvar lista de repositórios ativos em CSV"""
    if not repositories:
        print("\n⚠️  Nenhum repositório ativo para salvar")
        return
    
    fieldnames = [
        'name',
        'slug',
        'project_key',
        'project_name',
        'description',
        'language',
        'size',
        'last_activity_at',
        'created_on',
        'updated_on',
        'is_private',
        'url',
        'clone_https',
        'clone_ssh'
    ]
    
    rows = []
    for repo in repositories:
        project = repo.get('project', {})
        links = repo.get('links', {})
        clone_links = links.get('clone', [])
        
        # Extrair URLs de clone
        clone_https = ''
        clone_ssh = ''
        for clone in clone_links:
            if clone.get('name') == 'https':
                clone_https = clone.get('href', '')
            elif clone.get('name') == 'ssh':
                clone_ssh = clone.get('href', '')
        
        row = {
            'name': repo.get('name', ''),
            'slug': repo.get('slug', ''),
            'project_key': project.get('key', 'N/A'),
            'project_name': project.get('name', 'N/A'),
            'description': repo.get('description', 'N/A'),
            'language': repo.get('language', 'N/A'),
            'size': repo.get('size', 0),
            'last_activity_at': repo.get('last_activity_at', ''),
            'created_on': repo.get('created_on', ''),
            'updated_on': repo.get('updated_on', ''),
            'is_private': repo.get('is_private', False),
            'url': links.get('html', {}).get('href', ''),
            'clone_https': clone_https,
            'clone_ssh': clone_ssh
        }
        rows.append(row)
    
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    
    print(f"\n✓ Lista de repositórios ativos salva em: {filename}")
    print(f"  Total de repositórios: {len(rows)}")


def print_summary(repositories):
    """Imprimir resumo dos repositórios ativos"""
    print(f"\n{'='*60}")
    print("📊 RESUMO DOS REPOSITÓRIOS ATIVOS")
    print(f"{'='*60}\n")
    
    if not repositories:
        print("Nenhum repositório ativo encontrado nos últimos 30 dias.")
        return
    
    print(f"Total de repositórios ativos: {len(repositories)}")
    
    # Agrupar por projeto
    by_project = {}
    for repo in repositories:
        project_name = repo.get('project', {}).get('name', 'Sem Projeto')
        if project_name not in by_project:
            by_project[project_name] = []
        by_project[project_name].append(repo)
    
    print(f"Total de projetos: {len(by_project)}")
    
    # Agrupar por linguagem
    by_language = {}
    for repo in repositories:
        lang = repo.get('language', 'N/A')
        by_language[lang] = by_language.get(lang, 0) + 1
    
    print(f"\nRepositórios por linguagem:")
    for lang, count in sorted(by_language.items(), key=lambda x: x[1], reverse=True):
        print(f"  • {lang}: {count}")
    
    print(f"\nRepositórios por projeto:")
    for project, repos in sorted(by_project.items(), key=lambda x: len(x[1]), reverse=True):
        print(f"  • {project}: {len(repos)} repositórios")
    
    print(f"\n{'='*60}")
    print("🔝 TOP 10 REPOSITÓRIOS MAIS RECENTES")
    print(f"{'='*60}\n")
    
    # Ordenar por última atividade
    sorted_repos = sorted(
        repositories,
        key=lambda x: x.get('last_activity_at', ''),
        reverse=True
    )[:10]
    
    for idx, repo in enumerate(sorted_repos, 1):
        last_activity = repo.get('last_activity_at', 'N/A')
        if last_activity != 'N/A':
            try:
                dt = datetime.fromisoformat(last_activity.replace('Z', '+00:00'))
                last_activity = dt.strftime('%Y-%m-%d %H:%M:%S')
            except:
                pass
        
        print(f"{idx}. {repo.get('name')}")
        print(f"   Projeto: {repo.get('project', {}).get('name', 'N/A')}")
        print(f"   Última atividade: {last_activity}")
        print(f"   URL: {repo.get('links', {}).get('html', {}).get('href', 'N/A')}")
        print()


def main():
    """Função principal"""
    print("\n" + "="*60)
    print("🚀 LISTAGEM DE REPOSITÓRIOS ATIVOS - BITBUCKET")
    print("   Últimos 30 dias")
    print("="*60)
    
    # Verificar credenciais
    if not BITBUCKET_USERNAME or not BITBUCKET_API_TOKEN:
        print("\n⚠️  ATENÇÃO: Credenciais não configuradas!")
        print("\nPara usar este script, configure as variáveis de ambiente:")
        print("  export BITBUCKET_WORKSPACE='seu_workspace'")
        print("  export BITBUCKET_USERNAME='seu_usuario'")
        print("  export BITBUCKET_API_TOKEN='seu_api_token'")
        print("\nComo criar um API Token:")
        print("  1. Acesse: https://bitbucket.org/account/settings/")
        print("  2. Vá em 'Personal settings' → 'API tokens'")
        print("  3. Clique em 'Create token'")
        print("  4. Selecione os escopos: repository:read, project:read")
        print("  5. Copie o token gerado")
        sys.exit(1)
    
    # Criar sessão HTTP com autenticação
    session = requests.Session()
    session.auth = HTTPBasicAuth(BITBUCKET_USERNAME, BITBUCKET_API_TOKEN)
    
    # Conectar ao Bitbucket (equivalente ao gl.auth())
    user = test_authentication(session, BITBUCKET_WORKSPACE)
    
    # Calcular data de 30 dias atrás
    thirty_days_ago = datetime.now(pytz.UTC) - timedelta(days=30)
    thirty_days_ago_str = thirty_days_ago.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    
    try:
        # Buscar repositórios ativos
        active_repos = get_active_repositories(session, BITBUCKET_WORKSPACE, thirty_days_ago)
        
        if active_repos:
            print(f"\n✓ Total de repositórios ativos: {len(active_repos)}")
            
            # Gerar CSV
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            csv_filename = f"bitbucket_repos_ativos_{timestamp}.csv"
            
            save_to_csv(active_repos, csv_filename)
            
            # Imprimir resumo
            print_summary(active_repos)
        else:
            print("\n⚠️  Nenhum repositório ativo encontrado nos últimos 30 dias.")
        
        print(f"\n{'='*60}")
        print("✅ ANÁLISE CONCLUÍDA COM SUCESSO!")
        print(f"{'='*60}\n")
        
    except Exception as e:
        print(f"\n❌ Erro durante a análise: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
