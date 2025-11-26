#!/usr/bin/env python3
"""
Script para aplicar normalização automática de nomes/emails em CSVs do Bitbucket
Usa o mapeamento existente de person_mapping.json e pergunta apenas para novos nomes
"""

import pandas as pd
import os
import json
from pathlib import Path
from collections import defaultdict


def load_existing_mapping(mapping_file='person_mapping.json'):
    """Carrega o mapeamento existente"""
    if not os.path.exists(mapping_file):
        print(f"❌ Arquivo {mapping_file} não encontrado!")
        print("   Criando mapeamento vazio...")
        return {}, 1
    
    with open(mapping_file, 'r') as f:
        saved_data = json.load(f)
        person_to_id = {tuple(k.split('|||')): v for k, v in saved_data.get('mapping', {}).items()}
        next_person_num = saved_data.get('next_person_num', 1)
    
    print(f"✓ Mapeamento carregado: {len(person_to_id)} pessoas mapeadas")
    return person_to_id, next_person_num


def find_csv_files(patterns=['bitbucket_commits_details*.csv', 'bitbucket_prs_details*.csv', 'bitbucket_pipelines_details*.csv']):
    """Encontra todos os CSVs de commits, PRs e Pipelines do Bitbucket"""
    csv_files = []
    
    for root, dirs, files in os.walk('.'):
        # Ignorar pasta normalized
        if 'normalized' in root:
            continue
            
        for file in files:
            # Padrões do Bitbucket
            for pattern in patterns:
                if '*' not in pattern:
                    if file == pattern:
                        csv_files.append(os.path.join(root, file))
                    continue
                    
                pattern_prefix = pattern.split('*')[0]
                pattern_suffix = pattern.split('*')[1] if len(pattern.split('*')) > 1 else ''
                
                if file.startswith(pattern_prefix) and file.endswith(pattern_suffix):
                    csv_files.append(os.path.join(root, file))
                    break
    
    return sorted(set(csv_files))


def extract_people_from_csv(csv_file):
    """Extrai pessoas de um CSV específico (detecta automaticamente as colunas)"""
    people = set()
    
    try:
        df = pd.read_csv(csv_file)
        
        # Bitbucket Commits: author + author_email
        if 'author' in df.columns and 'author_email' in df.columns:
            for _, row in df.iterrows():
                author = str(row['author']).strip()
                email = str(row['author_email']).strip().lower()
                if author != 'nan' and author != '' and email != 'nan' and email != '':
                    people.add((author, email))
        
        # Bitbucket Pipelines: creator + creator_username
        elif 'creator' in df.columns and 'creator_username' in df.columns:
            for _, row in df.iterrows():
                creator = str(row['creator']).strip()
                username = str(row['creator_username']).strip().lower()
                if creator != 'nan' and creator != '' and creator != 'Unknown' and username != 'nan' and username != '':
                    people.add((creator, username))
        
        # Bitbucket PRs: author (sem email direto, mas pode ter na lista de reviewers)
        elif 'author' in df.columns:
            for _, row in df.iterrows():
                author = str(row['author']).strip()
                if author != 'nan' and author != '':
                    people.add((author, author.lower()))
            
            # Extrair reviewers da coluna reviewers_list se existir
            if 'reviewers_list' in df.columns:
                for _, row in df.iterrows():
                    reviewers_str = str(row['reviewers_list']).strip()
                    if reviewers_str != 'nan' and reviewers_str != '':
                        reviewers = [r.strip() for r in reviewers_str.split(',')]
                        for reviewer in reviewers:
                            if reviewer and reviewer != 'nan':
                                people.add((reviewer, reviewer.lower()))
        
    except Exception as e:
        print(f"  ⚠️  Erro ao ler {csv_file}: {e}")
    
    return people


def handle_new_people(new_people, person_to_id, next_person_num, auto_fill=False):
    """Processa pessoas não mapeadas - pergunta ou preenche com P n/a"""
    if not new_people:
        return person_to_id, next_person_num
    
    print(f"\n⚠️  Encontradas {len(new_people)} pessoas não mapeadas:")
    print("="*80)
    print(f"📌 Último Pn disponível no mapeamento: P{next_person_num - 1}")
    print(f"📌 Próximo Pn disponível para criar: P{next_person_num}")
    print("="*80)
    
    for idx, (name, email) in enumerate(sorted(new_people), 1):
        print(f"\n[{idx}/{len(new_people)}]")
        print(f"Nome:  {name}")
        print(f"Email/Username: {email}")
        print(f"💡 Próximo Pn livre: P{next_person_num}")
        
        if auto_fill:
            # Modo automático: preenche com "P n/a"
            person_to_id[(name, email)] = "P n/a"
            print("  → Preenchido automaticamente: P n/a")
        else:
            # Modo interativo
            while True:
                print("\nOpções:")
                print(f"  - Digite 'new' ou ENTER para criar novo: P{next_person_num}")
                print("  - Digite Pn existente para vincular (ex: P5)")
                print("  - Digite 'skip' para marcar como P n/a")
                
                response = input("\nSua escolha: ").strip()
                
                if response == '' or response.lower() == 'new':
                    # Criar novo Pn
                    anon_id = f"P{next_person_num}"
                    person_to_id[(name, email)] = anon_id
                    print(f"  ✅ Criado novo: {anon_id}")
                    next_person_num += 1
                    break
                
                elif response.lower() == 'skip':
                    person_to_id[(name, email)] = "P n/a"
                    print("  → Marcado como: P n/a")
                    break
                
                elif response.upper().startswith('P'):
                    # Vincular a Pn existente
                    try:
                        num_part = response[1:].strip()
                        
                        # Verificar se é P n/a
                        if num_part == 'n/a' or num_part.replace(' ', '') == 'n/a':
                            person_to_id[(name, email)] = "P N/A"
                            print(f"  → Marcado como: P N/A")
                            break
                        
                        # Verificar se é um número válido
                        if num_part.isdigit():
                            pn_number = int(num_part)
                            anon_id = f"P{pn_number}"
                            
                            # Verificar se este Pn existe no mapeamento
                            pn_exists = any(v == anon_id for v in person_to_id.values())
                            
                            if pn_exists:
                                person_to_id[(name, email)] = anon_id
                                print(f"  ✅ Vinculado a Pn existente: {anon_id}")
                                break
                            else:
                                print(f"  ⚠️  {anon_id} não existe no mapeamento.")
                                create = input(f"     Deseja criar {anon_id}? (s/n): ").strip().lower()
                                if create == 's':
                                    person_to_id[(name, email)] = anon_id
                                    # Atualizar next_person_num se necessário
                                    if pn_number >= next_person_num:
                                        next_person_num = pn_number + 1
                                    print(f"  ✅ Criado novo: {anon_id}")
                                    break
                                # Caso contrário, continua o loop
                        else:
                            print("  ⚠️  Formato inválido. Use P1, P2, ... ou P n/a")
                    except:
                        print("  ⚠️  Formato inválido. Use P1, P2, ... ou 'new' ou 'skip'")
                else:
                    print("  ⚠️  Digite 'new', 'skip', ou Pn (ex: P5)")
    
    return person_to_id, next_person_num


def apply_normalization_to_csv(csv_file, person_to_id, output_dir='normalized'):
    """Aplica normalização a um CSV específico"""
    os.makedirs(output_dir, exist_ok=True)
    
    try:
        df = pd.read_csv(csv_file)
        
        # Bitbucket Commits: author + author_email
        if 'author' in df.columns and 'author_email' in df.columns:
            df['anonymized_name'] = df.apply(
                lambda row: person_to_id.get(
                    (str(row['author']).strip(), str(row['author_email']).strip().lower()),
                    'P n/a'
                ) if str(row['author']).strip() != 'nan' and str(row['author']).strip() != '' else '',
                axis=1
            )
            
            # REMOVER colunas originais para evitar duplicação
            df = df.drop(columns=['author', 'author_email'])
            
            # Preservar estrutura de pastas
            relative_path = os.path.relpath(csv_file, '.')
            output_file = os.path.join(output_dir, relative_path)
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            # Salvar
            df.to_csv(output_file, index=False)
            return True, output_file
        
        # Bitbucket Pipelines: creator + creator_username
        elif 'creator' in df.columns and 'creator_username' in df.columns:
            # Normalizar creator
            def normalize_creator(row):
                creator = str(row['creator']).strip()
                username = str(row['creator_username']).strip().lower()
                
                if creator == 'nan' or creator == '' or creator == 'Unknown':
                    return ''
                
                # Procurar no mapeamento
                anon_id = person_to_id.get((creator, username), None)
                if anon_id:
                    return anon_id
                
                # Se não encontrou, tentar apenas pelo nome
                for (mapped_name, mapped_email), anon_id in person_to_id.items():
                    if mapped_name == creator:
                        return anon_id
                
                return 'P n/a'
            
            df['anonymized_creator'] = df.apply(normalize_creator, axis=1)
            
            # REMOVER colunas originais para evitar duplicação
            df = df.drop(columns=['creator', 'creator_username'])
            
            # Preservar estrutura de pastas
            relative_path = os.path.relpath(csv_file, '.')
            output_file = os.path.join(output_dir, relative_path)
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            # Salvar
            df.to_csv(output_file, index=False)
            return True, output_file
        
        # Bitbucket PRs: author + reviewers_list
        elif 'author' in df.columns and 'pr_id' in df.columns:
            # Normalizar author
            def normalize_author(author):
                author = str(author).strip()
                if author == 'nan' or author == '':
                    return ''
                
                # Procurar no mapeamento pelo nome
                for (mapped_name, mapped_email), anon_id in person_to_id.items():
                    if mapped_name == author:
                        return anon_id
                
                return 'P n/a'
            
            df['anonymized_author'] = df['author'].apply(normalize_author)
            
            # REMOVER coluna original author
            df = df.drop(columns=['author'])
            
            # Normalizar reviewers_list (se existir)
            if 'reviewers_list' in df.columns:
                def normalize_reviewers(reviewers_str):
                    if pd.isna(reviewers_str) or str(reviewers_str).strip() == '' or str(reviewers_str).strip() == 'nan':
                        return ''
                    
                    reviewers = str(reviewers_str).split(',')
                    anonymized_reviewers = []
                    
                    for reviewer in reviewers:
                        reviewer = reviewer.strip()
                        if reviewer and reviewer != 'nan':
                            # Procurar no mapeamento pelo nome
                            found = False
                            for (mapped_name, mapped_email), anon_id in person_to_id.items():
                                if mapped_name == reviewer:
                                    anonymized_reviewers.append(anon_id)
                                    found = True
                                    break
                            
                            if not found:
                                anonymized_reviewers.append('P n/a')
                    
                    return ', '.join(anonymized_reviewers)
                
                df['anonymized_reviewers'] = df['reviewers_list'].apply(normalize_reviewers)
                
                # REMOVER coluna original reviewers_list
                df = df.drop(columns=['reviewers_list'])
            
            # Preservar estrutura de pastas
            relative_path = os.path.relpath(csv_file, '.')
            output_file = os.path.join(output_dir, relative_path)
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            # Salvar
            df.to_csv(output_file, index=False)
            return True, output_file
        
        else:
            available_columns = list(df.columns)
            return False, f"Colunas de autor não encontradas. Colunas: {', '.join(available_columns[:5])}..."
            
    except Exception as e:
        return False, f"Erro: {str(e)}"


def save_updated_mapping(person_to_id, next_person_num, mapping_file='person_mapping.json'):
    """Salva o mapeamento atualizado"""
    save_mapping = {'|||'.join(k): v for k, v in person_to_id.items()}
    with open(mapping_file, 'w') as f:
        json.dump({
            'mapping': save_mapping,
            'next_person_num': next_person_num
        }, f, indent=2)
    print(f"\n✓ Mapeamento atualizado salvo em {mapping_file}")


def show_summary(results):
    """Mostra resumo do processamento"""
    print("\n" + "="*80)
    print("📊 RESUMO DO PROCESSAMENTO")
    print("="*80)
    
    success = [r for r in results if r['success']]
    failed = [r for r in results if not r['success']]
    
    print(f"\n✅ Processados com sucesso: {len(success)}")
    print(f"❌ Falhas: {len(failed)}")
    
    if failed:
        print("\nArquivos com falha:")
        for r in failed:
            print(f"  - {os.path.basename(r['file'])}: {r['message']}")


def main():
    print("🔧 APLICADOR DE NORMALIZAÇÃO AUTOMÁTICA (Bitbucket)")
    print("="*80 + "\n")
    
    # Carregar mapeamento existente
    person_to_id, next_person_num = load_existing_mapping()
    if person_to_id is None:
        return
    
    # Encontrar CSVs de Commits e PRs do Bitbucket
    print("\n📁 Buscando arquivos CSV do Bitbucket...")
    csv_files = find_csv_files()
    
    if not csv_files:
        print("❌ Nenhum arquivo CSV do Bitbucket encontrado!")
        print("   Procurando por: bitbucket_commits_details*.csv e bitbucket_prs_details*.csv")
        return
    
    print(f"✓ Encontrados {len(csv_files)} arquivos CSV\n")
    
    # Extrair todas as pessoas de todos os CSVs
    print("🔍 Extraindo pessoas de todos os CSVs...")
    all_people = set()
    for csv_file in csv_files:
        people = extract_people_from_csv(csv_file)
        all_people.update(people)
        if people:
            print(f"  ✓ {os.path.basename(csv_file)}: {len(people)} pessoas")
    
    print(f"\n✓ Total de {len(all_people)} combinações únicas encontradas")
    
    # Verificar pessoas não mapeadas
    mapped_people = set(person_to_id.keys())
    new_people = all_people - mapped_people
    
    if new_people:
        print(f"\n⚠️  {len(new_people)} pessoas novas precisam ser mapeadas")
        print("\nOpções:")
        print("  1. Preencher automaticamente com 'P n/a' (você pode editar depois)")
        print("  2. Mapear manualmente agora")
        
        choice = input("\nEscolha (1 ou 2): ").strip()
        auto_fill = (choice == '1')
        
        person_to_id, next_person_num = handle_new_people(
            new_people, person_to_id, next_person_num, auto_fill
        )
        
        # Salvar mapeamento atualizado
        save_updated_mapping(person_to_id, next_person_num)
    else:
        print("✓ Todas as pessoas já estão mapeadas!")
    
    # Aplicar normalização a todos os CSVs
    print("\n📝 Aplicando normalização aos CSVs...")
    results = []
    
    for csv_file in csv_files:
        print(f"\n  Processando: {os.path.basename(csv_file)}")
        success, message = apply_normalization_to_csv(csv_file, person_to_id)
        
        results.append({
            'file': csv_file,
            'success': success,
            'message': message
        })
        
        if success:
            print(f"    ✓ Salvo: {os.path.basename(message)}")
        else:
            print(f"    ❌ Erro: {message}")
    
    # Mostrar resumo
    show_summary(results)
    
    print("\n✅ Processamento concluído!")
    print(f"   Arquivos salvos em: ./normalized/")
    print("\n📋 Estrutura:")
    print("   - Bitbucket Commits: coluna 'anonymized_name'")
    print("   - Bitbucket PRs: colunas 'anonymized_author' + 'anonymized_reviewers'")
    print("   - Bitbucket Pipelines: coluna 'anonymized_creator'")


if __name__ == "__main__":
    main()
