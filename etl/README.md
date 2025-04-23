# Pipeline ETL - Teste Técnico Analista de Dados

Este projeto implementa um pipeline ETL (Extract, Transform, Load) que lê dados de um arquivo Excel, realiza transformações e carrega os dados em um banco PostgreSQL.

## Estrutura do Projeto

- `etl.py`: Script Python principal que executa todo o pipeline ETL
- `loja_db_dump.sql`: Dump SQL do banco de dados com as tabelas e dados
- `README.md`: Este arquivo com instruções de execução

## Requisitos

- Python 3.6+
- PostgreSQL 12+
- Bibliotecas Python:
  - pandas
  - openpyxl
  - psycopg2
  - numpy

## Instalação das Dependências

```bash
pip install pandas openpyxl psycopg2-binary numpy
```

## Configuração do Banco de Dados

O script está configurado para se conectar ao PostgreSQL com as seguintes credenciais:

- Host: localhost
- Porta: 5432
- Usuário: postgres
- Senha: postgres
- Banco de dados: loja_db

Se necessário, você pode modificar estas configurações no script `etl.py`.

## Execução do Pipeline ETL

1. Certifique-se de que o PostgreSQL está instalado e em execução
2. Coloque o arquivo Excel `vendas_loja_completa.xlsx` no diretório correto (ou atualize o caminho no script)
3. Execute o script ETL:

```bash
python etl.py
```

## Detalhes do Pipeline ETL

### 1. Extração (Extract)

O script extrai dados de duas abas do arquivo Excel:
- `clientes`: Contém informações dos clientes
- `vendas`: Contém as transações de vendas

### 2. Transformação (Transform)

As seguintes transformações são aplicadas aos dados:

**Tabela de Clientes:**
- Tratamento de valores nulos em `tipo_pessoa` (preenchidos com 'FISICA')
- Padronização de `tipo_pessoa` (convertido para maiúsculas)
- Tratamento de valores nulos em `tipo_contato` (preenchidos com 'email')
- Padronização de `tipo_contato` (convertido para minúsculas)
- Formatação de documentos (CPF/CNPJ)
- Adição de campo `data_cadastro`

**Tabela de Vendas:**
- Arredondamento de valores para 2 casas decimais
- Extração de mês e ano da data de venda
- Adição de campo `status_venda` (todas como 'CONCLUÍDA')

### 3. Carga (Load)

Os dados transformados são carregados em duas tabelas no PostgreSQL:

**Tabela `clientes`:**
```sql
CREATE TABLE clientes (
    id_cliente INTEGER PRIMARY KEY,
    nome VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL,
    documento VARCHAR(20),
    documento_formatado VARCHAR(20),
    tipo_pessoa VARCHAR(10) NOT NULL,
    tipo_contato VARCHAR(10) NOT NULL,
    data_cadastro DATE NOT NULL
);
```

**Tabela `vendas`:**
```sql
CREATE TABLE vendas (
    id_venda INTEGER PRIMARY KEY,
    id_cliente INTEGER NOT NULL,
    data_venda DATE NOT NULL,
    valor NUMERIC(10, 2) NOT NULL,
    mes_venda INTEGER NOT NULL,
    ano_venda INTEGER NOT NULL,
    status_venda VARCHAR(20) NOT NULL,
    FOREIGN KEY (id_cliente) REFERENCES clientes (id_cliente)
);
```

## Restauração do Banco de Dados a partir do Dump

Para restaurar o banco de dados a partir do dump SQL:

```bash
# Criar o banco de dados (se não existir)
createdb -U postgres loja_db

# Restaurar o dump
psql -U postgres -d loja_db -f loja_db_dump.sql
```

## Análises Possíveis

Com os dados carregados no PostgreSQL, é possível realizar diversas análises, como:

1. Total de vendas por mês/ano
2. Valor médio de vendas por cliente
3. Distribuição de clientes por tipo de pessoa (física/jurídica)
4. Preferência de contato dos clientes (email/celular)
5. Identificação de clientes com maior volume de compras

## Observações

- O script trata automaticamente valores nulos e formata documentos
- A chave estrangeira entre vendas e clientes garante a integridade referencial
- O script usa a estratégia ON CONFLICT para evitar duplicações em caso de reexecução
