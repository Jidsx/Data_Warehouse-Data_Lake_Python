# Exercício de Data Warehouse/Data Lake com Google Colab usando Python

## Data Warehouse

O ETL (Extract, Transform, Load) é um processo composto por uma série de atividades. Ele começa com a extração de dados de diversas fontes. Em seguida, esses dados são transformados para atender aos requisitos de qualidade e estrutura necessários. Por fim, os dados são carregados em um destino, como um data warehouse, onde podem ser armazenados e utilizados para análise e tomada de decisões.

~~~python
import pandas as pd
import numpy as np

# Criando o número total de produtos em um dicionário
num_produtos = 600
produtos = {
    'produto_id': range(1, num_produtos + 1),
    'nome': [f'Produto {i}' for i in range(1, num_produtos + 1)],
    'categoria': np.random.choice(['Eletrônicos', 'Roupas', 'Alimentos'], num_produtos)
}

# Definindo DataFrame de produtos
df_produtos = pd.DataFrame(produtos)

# Criando o número de venda em um dicionário
num_vendas = 1000 #
data_vendas = {
    'data': np.random.choice(pd.date_range('2024-04-01', periods=30), num_vendas), #
    'produto_id': np.random.randint(1, num_produtos + 1, num_vendas),
    'quantidade': np.random.randint(50, 200, num_vendas), #
    'valor_total': np.random.randint(1000, 10000, num_vendas) #
}

# Definindo DataFrame de vendas
df_vendas = pd.DataFrame(data_vendas)

# Salvando produtos e vendas em um arquivo CSV
df_vendas.to_csv('vendas.csv', index=False)
df_produtos.to_csv('produtos.csv', index=False)

# Extração de dados de produtos e vendas a partir do arquivo CSV
df_vendas = pd.read_csv('vendas.csv')
df_produtos = pd.read_csv('produtos.csv')

# Mesclando os DataFrames de vendas e produtos
df_merge = pd.merge(df_vendas, df_produtos, on='produto_id', how='inner')

# Salvando o DataFrame mesclado em um arquivo CSV
df_merge.to_csv('data_warehouse.csv', index=False)

# Imprimindo os dados
df_warehouse = pd.read_csv('data_warehouse.csv')
print("Conteúdo do Data WareHouse: ")
print(df_warehouse)
~~~

![ware_1](https://github.com/Jidsx/Data_Warehouse-Data_Lake_Python/assets/113401757/c116f377-8972-4e44-a2af-a81d5db5ca03)


~~~python
import pandas as pd
import matplotlib.pyplot as plt

#
df_warehouse = pd.read_csv('data_warehouse.csv')

#
vendas_por_produto = df_warehouse.groupby('nome')[['quantidade', 'valor_total']].sum()
print("Análise de vendas por produto: ")
print(vendas_por_produto)

#
vendas_por_categoria = df_warehouse.groupby('categoria')[['quantidade', 'valor_total']].sum()
print("\nAnálise de vendas por categoria de produto: ")
print(vendas_por_categoria)

#
df_warehouse['data'] = pd.to_datetime(df_warehouse['data'])
vendas_por_data = df_warehouse.groupby('data')[['quantidade', 'valor_total']].sum()
print("\nAnálise de tendências temporis: ")
print(vendas_por_data)

#
plt.figure(figsize=(10, 5))
plt.plot(vendas_por_data.index, vendas_por_data['quantidade'], marker='o', linestyle='-')
plt.title('vendas ao longo do tempo')
plt.xlabel('Data')
plt.ylabel('Quantidade Vendida')
plt.grid(True)
plt.show()

#
desempenho_produto = df_warehouse.groupby('nome')['valor_total'].sum()
print("\nAnálise de desempenho de produtos: ")
print(desempenho_produto)
~~~
![ware_2](https://github.com/Jidsx/Data_Warehouse-Data_Lake_Python/assets/113401757/7916e7c1-383c-4b02-a352-e1021c5969d9)
![ware_3](https://github.com/Jidsx/Data_Warehouse-Data_Lake_Python/assets/113401757/286f10ba-80d7-4027-a43e-bf1f21657e05)
![ware_4](https://github.com/Jidsx/Data_Warehouse-Data_Lake_Python/assets/113401757/5c8c82d1-d0e9-4a72-90ab-188b1bf7dec9)


## Data Lake

Este código simula a geração de dados em um "data lake" e em seguida carrega esses dados para um banco de dados SQL, criando uma tabela para cada arquivo CSV.

~~~python
import pandas as pd
import numpy as np
import os

# Ele verifica se o diretório 'data_lake' existe e, se não existir, ele vai criar
if not os.path.exists('data_lake'):
  os.makedirs('data_lake')

# Números de arquivo e Números de linhas por arquivos
num_files = 10
num_rows_per_file = 1000

# Lista para armazenar os nomes dos arquivos
dfs = []

#  Um for (Loop) para gerar os dados e salvar em arquivos CSV
for i in range(num_files):
  # Geração de dados aleatórios para as colunas
  data = {
      'coluna1': np.random.randint(0, 100, num_rows_per_file),
      'coluna2': np.random.randn(num_rows_per_file),
      'coluna3': np.random.choice(['A', 'B', 'C'], num_rows_per_file)
  }

  # Criação do DataFrame a partir dos dados gerados
  df = pd.DataFrame(data)

  # Salvar o arquivo em CSV
  file_name = f'data_lake/dados_{i+1}.csv'
  df.to_csv(file_name, index=False)

  # Adiciona o nome do arquivo e o DataFrame à lista
  dfs.append((file_name, df))

print("Dados do Date Lake gerados com Sucesso!")

# Exibe os primeiros registros de cada DataFrame gerado
for file_name, df in dfs:
  print(f"\nDados do arquivo: {file_name}\n")
  print(df.head())

~~~

![lake_1](https://github.com/Jidsx/Data_Warehouse-Data_Lake_Python/assets/113401757/27b8178b-2b38-4e6d-9626-12fe9d170bc9)

~~~python
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine

# String de conexão com o banco de dados SQLite
conn_string = 'sqlite:///data.lake.db'
# Criação de uma engine para conectar ao banco de dados
engine = create_engine(conn_string)

# Nome da tabela que será lida do banco de dados
table_name = 'dados_1'
# Leitura dos dados da tabela para um DataFrame
#df = pd.read_sql_table(table_name, engine)

# Exibe as primeiras linhas do DataFrame
print("Primeiras linhas do DataFrame: ")
print(df.head())

# Exibe informações sobre o DataFrame, como tipos de dados e número de entradas não nulas
print("\n Informações sobre o DataFrame: ")
print(df.info())

# Exibe um resumo estatístico das colunas numéricas do DataFrame
print("\n Resumo estatístico do DataFrame: ")
print(df.describe())

# Criação de um gráfico de dispersão entre coluna1 e coluna2
plt.figure(figsize=(8, 6))
sns.scatterplot(x='coluna1', y='coluna2', data=df)
plt.title('Gráfico de dispersão entre coluna1 e coluna2')
plt.xlabel('coluna1')
plt.ylabel('coluna2')
plt.grid(True)
plt.show()

# Criação de um histograma da coluna1
plt.figure(figsize=(8, 6))
sns.histplot(df['coluna1'], bins=20, kde=True)
plt.title('Histograma da coluna1')
plt.xlabel('coluna1')
plt.ylabel('Frequência')
plt.grid(True)
plt.show()

# Criação de um boxplot da coluna3 em relação à coluna1
plt.figure(figsize=(8, 6))
sns.boxplot(x='coluna3', y='coluna1', data=df)
plt.title('Boxplot da coluna3 em relação à coluna1')
plt.xlabel('coluna3')
plt.ylabel('coluna1')
plt.grid(True)
plt.show()

~~~
![lake_2](https://github.com/Jidsx/Data_Warehouse-Data_Lake_Python/assets/113401757/d1e9cc10-336b-4dc1-a9f9-9ea6f201a8d5)

![lake_3](https://github.com/Jidsx/Data_Warehouse-Data_Lake_Python/assets/113401757/470645e5-18e0-48e3-bf21-16a615e4f35e)

![lake_4](https://github.com/Jidsx/Data_Warehouse-Data_Lake_Python/assets/113401757/2728db64-0101-4e2a-9c52-ca3970c2fae3)

![lake_5](https://github.com/Jidsx/Data_Warehouse-Data_Lake_Python/assets/113401757/77693f9e-e89e-4adf-8f83-3dee8c1e438a)

