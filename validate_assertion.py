import pandas as pd

# Carregar o CSV
df = pd.read_csv('commit_patterns_analysis.csv')

# Total de commits
total_commits = len(df)

# Commits classificados (não "other")
commits_with_type = len(df[df['type_pattern'] != 'other'])

# Commits classificados como "other"
commits_other = len(df[df['type_pattern'] == 'other'])

# Taxa de assertividade (commits com tipo definido)
assertion_rate = (commits_with_type / total_commits) * 100

print("=" * 70)
print("ANÁLISE DE ASSERTIVIDADE DA CLASSIFICAÇÃO DE COMMITS")
print("=" * 70)
print(f"\nTotal de commits: {total_commits:,}")
print(f"Commits com tipo atribuído: {commits_with_type:,}")
print(f"Commits sem tipo (other): {commits_other:,}")
print(f"\n📊 ASSERTION RATE: {assertion_rate:.2f}%")
print("=" * 70)

# Distribuição por tipo
print("\nDistribuição por tipo de commit:")
print("-" * 70)
type_counts = df['type_pattern'].value_counts()
for type_name, count in type_counts.items():
    percentage = (count / total_commits) * 100
    print(f"{type_name:40} {count:6,} ({percentage:5.2f}%)")

print("\n" + "=" * 70)

# Análise por fonte (bitbucket vs gitlab)
print("\nDistribuição por fonte:")
print("-" * 70)
source_counts = df.groupby('source')['type_pattern'].apply(lambda x: (x != 'other').sum())
source_total = df['source'].value_counts()
for source in source_counts.index:
    typed = source_counts[source]
    total = source_total[source]
    rate = (typed / total) * 100
    print(f"{source:20} Typed: {typed:6,}/{total:6,} ({rate:5.2f}%)")