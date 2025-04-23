#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script ETL para processamento de dados de vendas de loja
Lê dados de um arquivo Excel, realiza transformações e carrega em um banco PostgreSQL
"""

import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os
import sys
from datetime import datetime

def extrair_dados(arquivo_excel):
    """
    Extrai dados das abas 'clientes' e 'vendas' do arquivo Excel
    
    Args:
        arquivo_excel: Caminho para o arquivo Excel
        
    Returns:
        Tuple contendo os DataFrames de clientes e vendas
    """
    try:
        print(f"Extraindo dados do arquivo: {arquivo_excel}")
        df_clientes = pd.read_excel(arquivo_excel, sheet_name='clientes')
        df_vendas = pd.read_excel(arquivo_excel, sheet_name='vendas')
        
        print(f"Dados extraídos com sucesso:")
        print(f"- Clientes: {len(df_clientes)} registros")
        print(f"- Vendas: {len(df_vendas)} registros")
        
        return df_clientes, df_vendas
    except Exception as e:
        print(f"Erro ao extrair dados: {str(e)}")
        sys.exit(1)

def transformar_dados(df_clientes, df_vendas):
    """
    Realiza transformações nos dados extraídos
    
    Args:
        df_clientes: DataFrame com dados de clientes
        df_vendas: DataFrame com dados de vendas
        
    Returns:
        Tuple contendo os DataFrames transformados
    """
    print("Iniciando transformações nos dados...")
    
    # Cópia dos DataFrames para evitar modificações nos originais
    clientes = df_clientes.copy()
    vendas = df_vendas.copy()
    
    # Transformações na tabela de clientes
    
    # 1. Tratamento de valores nulos em tipo_pessoa
    print("- Tratando valores nulos em tipo_pessoa...")
    # Preenchendo valores nulos com 'FISICA' (valor padrão)
    clientes['tipo_pessoa'] = clientes['tipo_pessoa'].fillna('FISICA')
    
    # 2. Padronização de tipo_pessoa (maiúsculas)
    clientes['tipo_pessoa'] = clientes['tipo_pessoa'].str.upper()
    
    # 3. Tratamento de valores nulos em tipo_contato
    print("- Tratando valores nulos em tipo_contato...")
    # Preenchendo valores nulos com 'email' (valor padrão)
    clientes['tipo_contato'] = clientes['tipo_contato'].fillna('email')
    
    # 4. Padronização de tipo_contato (minúsculas)
    clientes['tipo_contato'] = clientes['tipo_contato'].str.lower()
    
    # 5. Validação e formatação de documentos (CPF/CNPJ)
    print("- Formatando documentos...")
    def formatar_documento(doc, tipo):
        if pd.isna(doc) or doc is None or doc == '':
            return None
        
        # Remove caracteres não numéricos
        doc_limpo = ''.join(filter(str.isdigit, str(doc)))
        
        # Formata CPF: 000.000.000-00
        if tipo == 'FISICA' and len(doc_limpo) == 11:
            return f"{doc_limpo[:3]}.{doc_limpo[3:6]}.{doc_limpo[6:9]}-{doc_limpo[9:]}"
        # Formata CNPJ: 00.000.000/0000-00
        elif tipo == 'JURIDICA' and len(doc_limpo) == 14:
            return f"{doc_limpo[:2]}.{doc_limpo[2:5]}.{doc_limpo[5:8]}/{doc_limpo[8:12]}-{doc_limpo[12:]}"
        else:
            return doc_limpo
    
    clientes['documento_formatado'] = clientes.apply(
        lambda row: formatar_documento(row['documento'], row['tipo_pessoa']), axis=1
    )
    
    # 6. Criação de campo data_cadastro (data atual)
    print("- Adicionando data de cadastro...")
    clientes['data_cadastro'] = datetime.now().strftime('%Y-%m-%d')
    
    # Transformações na tabela de vendas
    
    # 1. Arredondamento de valores para 2 casas decimais
    print("- Arredondando valores de vendas...")
    vendas['valor'] = vendas['valor'].round(2)
    
    # 2. Criação de campo para mês da venda
    print("- Extraindo mês e ano das vendas...")
    vendas['mes_venda'] = vendas['data_venda'].dt.month
    vendas['ano_venda'] = vendas['data_venda'].dt.year
    
    # 3. Criação de campo para status da venda (todas como 'CONCLUÍDA')
    print("- Adicionando status às vendas...")
    vendas['status_venda'] = 'CONCLUÍDA'
    
    print("Transformações concluídas com sucesso!")
    return clientes, vendas

def criar_tabelas(conn):
    """
    Cria as tabelas no banco de dados PostgreSQL
    
    Args:
        conn: Conexão com o banco de dados
    """
    print("Criando tabelas no PostgreSQL...")
    
    cursor = conn.cursor()
    
    # Criação da tabela de clientes
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS clientes (
        id_cliente INTEGER PRIMARY KEY,
        nome VARCHAR(100) NOT NULL,
        email VARCHAR(100) NOT NULL,
        documento VARCHAR(20),
        documento_formatado VARCHAR(20),
        tipo_pessoa VARCHAR(10) NOT NULL,
        tipo_contato VARCHAR(10) NOT NULL,
        data_cadastro DATE NOT NULL
    );
    """)
    
    # Criação da tabela de vendas
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS vendas (
        id_venda INTEGER PRIMARY KEY,
        id_cliente INTEGER NOT NULL,
        data_venda DATE NOT NULL,
        valor NUMERIC(10, 2) NOT NULL,
        mes_venda INTEGER NOT NULL,
        ano_venda INTEGER NOT NULL,
        status_venda VARCHAR(20) NOT NULL,
        FOREIGN KEY (id_cliente) REFERENCES clientes (id_cliente)
    );
    """)
    
    conn.commit()
    print("Tabelas criadas com sucesso!")

def carregar_dados(conn, df_clientes, df_vendas):
    """
    Carrega os dados transformados no banco de dados PostgreSQL
    
    Args:
        conn: Conexão com o banco de dados
        df_clientes: DataFrame de clientes transformado
        df_vendas: DataFrame de vendas transformado
    """
    print("Carregando dados no PostgreSQL...")
    
    cursor = conn.cursor()
    
    # Inserção dos dados de clientes
    print("- Inserindo dados de clientes...")
    for _, row in df_clientes.iterrows():
        cursor.execute("""
        INSERT INTO clientes (id_cliente, nome, email, documento, documento_formatado, tipo_pessoa, tipo_contato, data_cadastro)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id_cliente) DO UPDATE SET
            nome = EXCLUDED.nome,
            email = EXCLUDED.email,
            documento = EXCLUDED.documento,
            documento_formatado = EXCLUDED.documento_formatado,
            tipo_pessoa = EXCLUDED.tipo_pessoa,
            tipo_contato = EXCLUDED.tipo_contato,
            data_cadastro = EXCLUDED.data_cadastro;
        """, (
            row['id_cliente'],
            row['nome'],
            row['email'],
            row['documento'],
            row['documento_formatado'],
            row['tipo_pessoa'],
            row['tipo_contato'],
            row['data_cadastro']
        ))
    
    # Inserção dos dados de vendas
    print("- Inserindo dados de vendas...")
    for _, row in df_vendas.iterrows():
        cursor.execute("""
        INSERT INTO vendas (id_venda, id_cliente, data_venda, valor, mes_venda, ano_venda, status_venda)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id_venda) DO UPDATE SET
            id_cliente = EXCLUDED.id_cliente,
            data_venda = EXCLUDED.data_venda,
            valor = EXCLUDED.valor,
            mes_venda = EXCLUDED.mes_venda,
            ano_venda = EXCLUDED.ano_venda,
            status_venda = EXCLUDED.status_venda;
        """, (
            row['id_venda'],
            row['id_cliente'],
            row['data_venda'],
            row['valor'],
            row['mes_venda'],
            row['ano_venda'],
            row['status_venda']
        ))
    
    conn.commit()
    print("Dados carregados com sucesso!")

def criar_banco_dados():
    """
    Cria o banco de dados PostgreSQL se não existir
    
    Returns:
        Conexão com o banco de dados
    """
    print("Configurando banco de dados PostgreSQL...")
    
    # Conecta ao PostgreSQL
    conn = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="postgres",
        port="5432",
        connect_timeout=10
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    
    cursor = conn.cursor()
    
    # Verifica se o banco de dados já existe
    cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'loja_db'")
    exists = cursor.fetchone()
    
    if not exists:
        print("Criando banco de dados 'loja_db'...")
        cursor.execute("CREATE DATABASE loja_db")
        print("Banco de dados criado com sucesso!")
    else:
        print("Banco de dados 'loja_db' já existe.")
    
    # Fecha a conexão atual
    cursor.close()
    conn.close()
    
    # Conecta ao banco de dados criado
    conn = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="postgres",
        port="5432",
        database="loja_db",
        connect_timeout=10
    )
    
    return conn

def main():
    """
    Função principal que executa o pipeline ETL completo
    """
    print("Iniciando pipeline ETL...")
    
    # Caminho do arquivo Excel
    arquivo_excel = '/home/ubuntu/upload/vendas_loja_completa.xlsx'
    
    # Extração
    df_clientes, df_vendas = extrair_dados(arquivo_excel)
    
    # Transformação
    df_clientes_transformado, df_vendas_transformado = transformar_dados(df_clientes, df_vendas)
    
    try:
        # Criação do banco de dados e conexão
        conn = criar_banco_dados()
        
        # Criação das tabelas
        criar_tabelas(conn)
        
        # Carregamento dos dados
        carregar_dados(conn, df_clientes_transformado, df_vendas_transformado)
        
        # Fecha a conexão
        conn.close()
        
        print("Pipeline ETL executado com sucesso!")
        
    except Exception as e:
        print(f"Erro durante a execução do pipeline ETL: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
