#!/usr/bin/env python3
"""
Script para extrair métricas detalhadas de repositórios ativos do GitLab
Lê o CSV de repositórios ativos e coleta commits, MRs e pipelines
Salva os dados em tempo real à medida que processa cada repositório
"""

import gitlab
from datetime import datetime, timedelta
import pytz
from dotenv import load_dotenv
import os
import csv
import time
import argparse
from collections import defaultdict

# Carregando variáveis de ambiente do arquivo config.env
load_dotenv('config.env')
TOKEN = os.getenv('TOKEN')

# Conectar ao GitLab
gl = gitlab.Gitlab('https://gitlab.com', private_token=TOKEN)
gl.auth()

# Teste de autenticação
user = gl.user
print(f"✓ Logado como: {user.username}")
print(f"{'='*60}")

# Configurar argumentos de linha de comando
parser = argparse.ArgumentParser(
    description='Extrair métricas detalhadas de repositórios GitLab em um período específico',
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog="""
Exemplos de uso:
  # Últimos 30 dias (padrão)
  python extract_metrics_from_active_repos.py
  
  # Período específico
  python extract_metrics_from_active_repos.py --start-date 2024-09-01 --end-date 2024-09-30
  
  # Últimos 90 dias
  python extract_metrics_from_active_repos.py --days 90
  
  # Com arquivo CSV específico
  python extract_metrics_from_active_repos.py --input-csv meu_arquivo.csv --days 60
    """
)

parser.add_argument(
    '--start-date',
    type=str,
    help='Data inicial no formato YYYY-MM-DD (ex: 2024-09-01)'
)

parser.add_argument(
    '--end-date',
    type=str,
    help='Data final no formato YYYY-MM-DD (ex: 2024-09-30). Se não especificada, usa a data atual'
)

parser.add_argument(
    '--days',
    type=int,
    default=30,
    help='Número de dias atrás a partir da data final (padrão: 30). Ignorado se --start-date for especificado'
)

parser.add_argument(
    '--input-csv',
    type=str,
    help='Arquivo CSV de entrada com repositórios ativos. Se não especificado, usa o mais recente'
)

args = parser.parse_args()

# Calcular datas do período
if args.start_date:
    try:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').replace(tzinfo=pytz.UTC)
    except ValueError:
        print("❌ Erro: Data inicial inválida. Use o formato YYYY-MM-DD")
        exit(1)
else:
    # Se não especificou start_date, calcular com base em --days
    if args.end_date:
        try:
            end_date = datetime.strptime(args.end_date, '%Y-%m-%d').replace(tzinfo=pytz.UTC)
        except ValueError:
            print("❌ Erro: Data final inválida. Use o formato YYYY-MM-DD")
            exit(1)
    else:
        end_date = datetime.now(pytz.UTC)
    
    start_date = end_date - timedelta(days=args.days)

if args.end_date:
    try:
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d').replace(hour=23, minute=59, second=59, tzinfo=pytz.UTC)
    except ValueError:
        print("❌ Erro: Data final inválida. Use o formato YYYY-MM-DD")
        exit(1)
else:
    end_date = datetime.now(pytz.UTC)

# Validar que start_date < end_date
if start_date >= end_date:
    print("❌ Erro: A data inicial deve ser anterior à data final")
    exit(1)

start_date_str = start_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
end_date_str = end_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')

# Nome do arquivo CSV de entrada
if args.input_csv:
    INPUT_CSV = args.input_csv
else:
    # Buscar o arquivo mais recente que corresponde ao padrão
    import glob
    csv_files = glob.glob('gitlab_active_repos_*.csv')
    # if csv_files:
    #     INPUT_CSV = max(csv_files, key=os.path.getctime)
    # else:
    INPUT_CSV = 'gitlab_active_repos-br_20251007_143531.csv'

# Gerar nomes dos arquivos de saída com as datas do período
start_date_formatted = start_date.strftime('%Y%m%d')
end_date_formatted = end_date.strftime('%Y%m%d')
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

# Create month and year folders based on current date
current_month = datetime.now().strftime('%m')
current_year = datetime.now().strftime('%Y')
month_names = {
    '01': '01-janeiro', '02': '02-fevereiro', '03': '03-marco',
    '04': '04-abril', '05': '05-maio', '06': '06-junho',
    '07': '07-julho', '08': '08-agosto', '09': '09-setembro',
    '10': '10-outubro', '11': '11-novembro', '12': '12-dezembro'
}

month_folder = month_names.get(current_month, f'{current_month}-unknown')
output_path = os.path.join(os.path.dirname(__file__), month_folder, current_year)

# Create directories if they don't exist
os.makedirs(output_path, exist_ok=True)

# Update file paths to include the new directory structure
COMMITS_CSV = os.path.join(output_path, f'gitlab_commits_{start_date_formatted}_to_{end_date_formatted}_{timestamp}.csv')
MRS_CSV = os.path.join(output_path, f'gitlab_mrs_{start_date_formatted}_to_{end_date_formatted}_{timestamp}.csv')
PIPELINES_CSV = os.path.join(output_path, f'gitlab_pipelines_{start_date_formatted}_to_{end_date_formatted}_{timestamp}.csv')
SUMMARY_CSV = os.path.join(output_path, f'gitlab_summary_{start_date_formatted}_to_{end_date_formatted}_{timestamp}.csv')

print(f"\n📊 Extração de Métricas Detalhadas - GitLab")
print(f"   Período: {start_date.strftime('%Y-%m-%d')} até {end_date.strftime('%Y-%m-%d')}")
print(f"   Dias: {(end_date - start_date).days} dias")
print(f"   Arquivo de entrada: {INPUT_CSV}")
print(f"   Modo: Salvamento em tempo real")
print(f"{'='*60}\n")

def parse_gitlab_datetime(datetime_str):
    """Parse GitLab API datetime string to datetime object."""
    if not datetime_str:
        return None
    try:
        # Try parsing with microseconds
        dt = datetime.strptime(datetime_str.split('.')[0], '%Y-%m-%dT%H:%M:%S')
    except ValueError:
        try:
            # If that fails, try without microseconds
            dt = datetime.strptime(datetime_str.split('+')[0], '%Y-%m-%dT%H:%M:%S')
        except ValueError:
            return None
    
    return dt.replace(tzinfo=pytz.UTC)

def init_csv_files():
    """Inicializa os arquivos CSV com os headers"""
    # Commits CSV
    with open(COMMITS_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'repository', 'repository_name', 'commit_id', 'commit_short_id', 
            'title', 'author', 'author_email', 'created_at', 'lines_added', 
            'lines_deleted', 'message'
        ])
    
    # MRs CSV
    with open(MRS_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'repository', 'repository_name', 'mr_iid', 'mr_id', 'title', 
            'state', 'author', 'author_username', 'created_at', 'updated_at', 
            'merged_at', 'closed_at', 'duration_hours', 'source_branch', 
            'target_branch', 'lines_added', 'lines_deleted', 'files_changed',
            'reviewers', 'reviewers_count', 'approvals_count', 'review_comments', 
            'upvotes', 'downvotes', 'web_url'
        ])
    
    # Pipelines CSV
    with open(PIPELINES_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'repository', 'repository_name', 'pipeline_id', 'status', 'ref', 
            'sha', 'created_at', 'updated_at', 'duration_minutes', 'web_url'
        ])
    
    # Summary CSV
    with open(SUMMARY_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'repository', 'repository_name', 'total_commits', 'total_contributors',
            'top_contributor', 'top_contributor_commits', 'total_mrs', 'mrs_open',
            'mrs_merged', 'mrs_closed', 'avg_mr_duration_hours', 'total_pipelines',
            'pipelines_success', 'pipelines_failed', 'pipelines_running',
            'last_activity', 'days_since_activity'
        ])

try:
    # Ler o CSV de repositórios ativos
    print("📂 Lendo arquivo CSV de repositórios ativos...")
    active_repos = []
    
    with open(INPUT_CSV, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        active_repos = list(reader)
    
    print(f"✓ {len(active_repos)} repositórios encontrados no CSV\n")
    
    # Inicializar arquivos CSV
    print("📝 Inicializando arquivos CSV...")
    init_csv_files()
    print(f"✓ Commits: {COMMITS_CSV}")
    print(f"✓ Merge Requests: {MRS_CSV}")
    print(f"✓ Pipelines: {PIPELINES_CSV}")
    print(f"✓ Resumo: {SUMMARY_CSV}\n")
    
    print(f"{'='*60}")
    print("COLETANDO MÉTRICAS DETALHADAS")
    print(f"{'='*60}\n")
    
    # Contadores globais para estatísticas
    global_stats = {
        'total_commits': 0,
        'total_mrs': 0,
        'total_pipelines': 0,
        'commit_authors': defaultdict(int)
    }
    
    for idx, repo_info in enumerate(active_repos, 1):
        full_path = repo_info['full_path']
        repo_name = repo_info['name']
        
        print(f"[{idx}/{len(active_repos)}] Processando: {full_path}")
        print("-" * 60)
        
        try:
            # Buscar o projeto completo usando full_path
            project = gl.projects.get(full_path)
            
            # ==================== COMMITS ====================
            print("   📝 Coletando commits...", end=" ", flush=True)
            commits = []
            commit_authors = defaultdict(int)
            
            try:
                commits_list = project.commits.list(
                    since=start_date_str,
                    until=end_date_str,
                    get_all=True
                )
                
                # Abrir arquivo de commits em modo append
                with open(COMMITS_CSV, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    
                    for commit in commits_list:
                        commit_date = parse_gitlab_datetime(commit.created_at)
                        
                        # Obter diff para calcular linhas adicionadas/removidas
                        try:
                            diff = commit.diff(get_all=True)
                            additions = 0
                            deletions = 0
                            
                            for d in diff:
                                if 'diff' in d:
                                    diff_lines = d['diff'].split('\n')
                                    for line in diff_lines:
                                        if line.startswith('+') and not line.startswith('+++'):
                                            additions += 1
                                        elif line.startswith('-') and not line.startswith('---'):
                                            deletions += 1
                        except:
                            additions = 0
                            deletions = 0
                        
                        author_name = commit.author_name if hasattr(commit, 'author_name') else 'Unknown'
                        commit_authors[author_name] += 1
                        global_stats['commit_authors'][author_name] += 1
                        
                        # Escrever linha no CSV imediatamente
                        writer.writerow([
                            full_path,
                            repo_name,
                            commit.id,
                            commit.short_id,
                            commit.title,
                            author_name,
                            commit.author_email if hasattr(commit, 'author_email') else '',
                            commit_date.strftime('%Y-%m-%d %H:%M:%S') if commit_date else '',
                            additions,
                            deletions,
                            commit.message[:200] if hasattr(commit, 'message') else ''
                        ])
                
                commits = commits_list
                global_stats['total_commits'] += len(commits)
                print(f"✓ {len(commits)} commits salvos")
                
            except Exception as e:
                print(f"⚠️ Erro: {str(e)[:50]}")
            
            # ==================== MERGE REQUESTS ====================
            print("   🔀 Coletando Merge Requests...", end=" ", flush=True)
            mrs = []
            mrs_by_state = defaultdict(int)
            avg_mr_duration = 0
            
            try:
                mrs_list = project.mergerequests.list(
                    updated_after=start_date_str,
                    updated_before=end_date_str,
                    get_all=True
                )
                
                # Abrir arquivo de MRs em modo append
                with open(MRS_CSV, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    mr_durations = []
                    
                    for mr in mrs_list:
                        created_at = parse_gitlab_datetime(mr.created_at)
                        updated_at = parse_gitlab_datetime(mr.updated_at)
                        merged_at = parse_gitlab_datetime(mr.merged_at) if hasattr(mr, 'merged_at') and mr.merged_at else None
                        closed_at = parse_gitlab_datetime(mr.closed_at) if hasattr(mr, 'closed_at') and mr.closed_at else None
                        
                        # Calcular duração
                        duration_hours = None
                        if created_at:
                            end_time = merged_at or closed_at or updated_at
                            if end_time:
                                duration_hours = (end_time - created_at).total_seconds() / 3600
                                if duration_hours > 0:
                                    mr_durations.append(duration_hours)
                        
                        mrs_by_state[mr.state] += 1
                        
                        # Contar comentários (apenas discussões de review)
                        review_comments = 0
                        try:
                            discussions = mr.discussions.list(get_all=True)
                            for discussion in discussions:
                                notes = discussion.notes.list(get_all=True)
                                review_notes = [note for note in notes if not note.system]
                                review_comments += len(review_notes)
                        except:
                            review_comments = 0
                        
                        # Obter alterações do MR (linhas e arquivos)
                        lines_added = 0
                        lines_deleted = 0
                        files_changed = 0
                        try:
                            changes = mr.changes()
                            changes_list = changes.get('changes', [])
                            files_changed = len(changes_list)
                            
                            for change in changes_list:
                                if 'diff' in change:
                                    diff_lines = change['diff'].split('\n')
                                    for line in diff_lines:
                                        if line.startswith('+') and not line.startswith('+++'):
                                            lines_added += 1
                                        elif line.startswith('-') and not line.startswith('---'):
                                            lines_deleted += 1
                        except:
                            pass
                        
                        # Obter reviewers e approvals
                        reviewers_list = []
                        approvals_count = 0
                        try:
                            # Buscar aprovações do MR
                            approvals = mr.approvals.get()
                            approvals_count = len(approvals.approved_by) if hasattr(approvals, 'approved_by') else 0
                            
                            # Coletar nomes dos reviewers que aprovaram
                            if hasattr(approvals, 'approved_by') and approvals.approved_by:
                                for approver in approvals.approved_by:
                                    if 'user' in approver and 'name' in approver['user']:
                                        reviewers_list.append(approver['user']['name'])
                        except:
                            # Se não conseguir obter approvals, tentar pegar reviewers de outra forma
                            try:
                                # Verificar se há reviewers atribuídos
                                if hasattr(mr, 'reviewers') and mr.reviewers:
                                    for reviewer in mr.reviewers:
                                        if 'name' in reviewer:
                                            reviewers_list.append(reviewer['name'])
                            except:
                                pass
                        
                        reviewers_str = '; '.join(reviewers_list) if reviewers_list else ''
                        reviewers_count = len(reviewers_list)
                        
                        # Escrever linha no CSV imediatamente
                        writer.writerow([
                            full_path,
                            repo_name,
                            mr.iid,
                            mr.id,
                            mr.title,
                            mr.state,
                            mr.author.get('name', 'Unknown') if hasattr(mr, 'author') else 'Unknown',
                            mr.author.get('username', '') if hasattr(mr, 'author') else '',
                            created_at.strftime('%Y-%m-%d %H:%M:%S') if created_at else '',
                            updated_at.strftime('%Y-%m-%d %H:%M:%S') if updated_at else '',
                            merged_at.strftime('%Y-%m-%d %H:%M:%S') if merged_at else '',
                            closed_at.strftime('%Y-%m-%d %H:%M:%S') if closed_at else '',
                            round(duration_hours, 2) if duration_hours else 0,
                            mr.source_branch if hasattr(mr, 'source_branch') else '',
                            mr.target_branch if hasattr(mr, 'target_branch') else '',
                            lines_added,
                            lines_deleted,
                            files_changed,
                            reviewers_str,
                            reviewers_count,
                            approvals_count,
                            review_comments,
                            mr.upvotes if hasattr(mr, 'upvotes') else 0,
                            mr.downvotes if hasattr(mr, 'downvotes') else 0,
                            mr.web_url if hasattr(mr, 'web_url') else ''
                        ])
                    
                    avg_mr_duration = sum(mr_durations) / len(mr_durations) if mr_durations else 0
                
                mrs = mrs_list
                global_stats['total_mrs'] += len(mrs)
                print(f"✓ {len(mrs)} MRs salvos")
                
            except Exception as e:
                print(f"⚠️ Erro: {str(e)[:50]}")
            
            # ==================== PIPELINES ====================
            print("   🔧 Coletando Pipelines...", end=" ", flush=True)
            pipelines = []
            pipeline_statuses = defaultdict(int)
            
            try:
                pipelines_list = project.pipelines.list(
                    updated_after=start_date_str,
                    updated_before=end_date_str,
                    get_all=True,
                    per_page=100
                )
                
                # Abrir arquivo de pipelines em modo append
                with open(PIPELINES_CSV, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    
                    for pipeline in pipelines_list:
                        created_at = parse_gitlab_datetime(pipeline.created_at) if hasattr(pipeline, 'created_at') else None
                        updated_at = parse_gitlab_datetime(pipeline.updated_at) if hasattr(pipeline, 'updated_at') else None
                        
                        # Filtrar por data de criação
                        if created_at and created_at < start_date:
                            continue
                        
                        status = pipeline.status if hasattr(pipeline, 'status') else ''
                        pipeline_statuses[status] += 1
                        
                        # Calcular duração
                        duration_minutes = None
                        if hasattr(pipeline, 'duration') and pipeline.duration:
                            duration_minutes = pipeline.duration / 60
                        
                        # Escrever linha no CSV imediatamente
                        writer.writerow([
                            full_path,
                            repo_name,
                            pipeline.id,
                            status,
                            pipeline.ref if hasattr(pipeline, 'ref') else '',
                            pipeline.sha[:8] if hasattr(pipeline, 'sha') else '',
                            created_at.strftime('%Y-%m-%d %H:%M:%S') if created_at else '',
                            updated_at.strftime('%Y-%m-%d %H:%M:%S') if updated_at else '',
                            round(duration_minutes, 2) if duration_minutes else 0,
                            pipeline.web_url if hasattr(pipeline, 'web_url') else ''
                        ])
                
                pipelines = pipelines_list
                global_stats['total_pipelines'] += len(pipelines)
                print(f"✓ {len(pipelines)} pipelines salvos")
                
            except Exception as e:
                print(f"⚠️ Erro: {str(e)[:50]}")
            
            # ==================== RESUMO DO REPOSITÓRIO ====================
            top_contributor = max(commit_authors.items(), key=lambda x: x[1])[0] if commit_authors else 'N/A'
            top_contributor_count = max(commit_authors.values()) if commit_authors else 0
            
            # Salvar resumo do repositório
            with open(SUMMARY_CSV, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    full_path,
                    repo_name,
                    len(commits),
                    len(commit_authors),
                    top_contributor,
                    top_contributor_count,
                    len(mrs),
                    mrs_by_state.get('opened', 0),
                    mrs_by_state.get('merged', 0),
                    mrs_by_state.get('closed', 0),
                    round(avg_mr_duration, 2),
                    len(pipelines),
                    pipeline_statuses.get('success', 0),
                    pipeline_statuses.get('failed', 0),
                    pipeline_statuses.get('running', 0),
                    repo_info['last_activity_at'],
                    repo_info['days_since_activity']
                ])
            
            print(f"   ✓ Resumo salvo: {len(commits)} commits, {len(mrs)} MRs, {len(pipelines)} pipelines\n")
            
            # Pequena pausa para evitar rate limiting
            time.sleep(0.5)
            
        except gitlab.exceptions.GitlabGetError as e:
            print(f"   ❌ Erro ao acessar projeto: {e}\n")
            continue
        except Exception as e:
            print(f"   ❌ Erro inesperado: {str(e)[:100]}\n")
            continue
    
    # ==================== ESTATÍSTICAS GERAIS ====================
    print(f"\n{'='*60}")
    print("📊 ESTATÍSTICAS GERAIS")
    print(f"{'='*60}\n")
    
    print(f"✅ Arquivos gerados:")
    print(f"   • {COMMITS_CSV}")
    print(f"   • {MRS_CSV}")
    print(f"   • {PIPELINES_CSV}")
    print(f"   • {SUMMARY_CSV}")
    
    print(f"\n📈 Totais:")
    print(f"Total de repositórios processados: {len(active_repos)}")
    print(f"Total de commits: {global_stats['total_commits']}")
    print(f"Total de Merge Requests: {global_stats['total_mrs']}")
    print(f"Total de Pipelines: {global_stats['total_pipelines']}")
    
    if global_stats['commit_authors']:
        print(f"\n🏆 Top 5 contribuidores:")
        top_authors = sorted(global_stats['commit_authors'].items(), key=lambda x: x[1], reverse=True)[:5]
        for idx, (author, count) in enumerate(top_authors, 1):
            print(f"  {idx}. {author}: {count} commits")

except FileNotFoundError:
    print(f"❌ Arquivo não encontrado: {INPUT_CSV}")
    print("   Certifique-se de executar o script list_active_repos.py primeiro")
except gitlab.exceptions.GitlabAuthenticationError:
    print("❌ Erro de autenticação!")
    print("   Verifique se o TOKEN está correto no arquivo config.env")
except Exception as e:
    print(f"❌ Erro inesperado: {e}")
    import traceback
    traceback.print_exc()

print(f"\n{'='*60}")
print("✅ Script concluído!")
print(f"{'='*60}\n")
