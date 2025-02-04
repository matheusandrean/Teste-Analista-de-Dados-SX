# Teste de Analista de Dados - SX Negócios

## 📌 Sobre o Projeto
Este projeto consiste na **extração, transformação e carregamento (ETL)** de dados do **ENEM 2020** utilizando **PySpark** e carregamento dos dados em um banco de dados **MySQL** hospedado em um container Docker. Além disso, foram criadas consultas SQL para responder a perguntas de negócio e visualizar os dados.

## 📂 Estrutura do Repositório
```
📂 sx_negocios_etl
│-- 📁 data
│   ├── microdados_enem_2020.csv  # Arquivo original do ENEM
│-- 📁 scripts
│   ├── etl.py                    # Script principal de ETL
│   ├── create_database.py        # Script de criação do banco de dados
│   ├── queries.py                # Script de consultas SQL
│-- 📁 notebooks
│   ├── eda.ipynb                 # Análise Exploratória dos Dados
│-- 📁 docs
│   ├── README.md                 # Documentação do projeto
│-- Dockerfile                     # Configuração do container Docker
│-- requirements.txt               # Dependências do projeto
```

## 🚀 Configuração do Ambiente
### **1️⃣ Instalar Docker**
```bash
docker --version
```
Rodar o container MySQL:
```bash
docker run --name mysql-enem -e MYSQL_ROOT_PASSWORD=root -e MYSQL_DATABASE=enem -p 3306:3306 -d mysql:latest
```

### **2️⃣ Instalar Python e Dependências**
```bash
pip install -r requirements.txt
```

---

## 📌 ETL - Extração, Transformação e Carga dos Dados

### **1️⃣ Criar o Banco de Dados MySQL** (`scripts/create_database.py`)
```python
import mysql.connector

def create_database():
    conn = mysql.connector.connect(host="localhost", user="root", password="root")
    cursor = conn.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS enem;")
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    create_database()
```

### **2️⃣ ETL: Processamento e Carga no MySQL** (`scripts/etl.py`)
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import mysql.connector

# Criando sessão Spark
spark = SparkSession.builder.appName("ENEM_ETL").getOrCreate()

# Carregar os dados
df = spark.read.csv("data/microdados_enem_2020.csv", header=True, sep=";", encoding="latin1")

# Selecionar e limpar dados
df = df.select(
    col("NU_INSCRICAO").alias("id_aluno"),
    col("CO_ESCOLA").alias("id_escola"),
    col("TP_SEXO").alias("sexo"),
    col("TP_COR_RACA").alias("etnia"),
    col("NU_NOTA_CN").alias("nota_cn"),
    col("NU_NOTA_CH").alias("nota_ch"),
    col("NU_NOTA_LC").alias("nota_lc"),
    col("NU_NOTA_MT").alias("nota_mt"),
    col("NU_NOTA_REDACAO").alias("nota_redacao")
)

# Preenchendo valores nulos
df = df.fillna(0)

# Criando nova coluna de nota total
df = df.withColumn("nota_total", col("nota_cn") + col("nota_ch") + col("nota_lc") + col("nota_mt"))

df_pandas = df.toPandas()

# Conectar ao MySQL
conn = mysql.connector.connect(host="localhost", user="root", password="root", database="enem")
cursor = conn.cursor()

# Criar tabela
cursor.execute('''
CREATE TABLE IF NOT EXISTS Fato_Notas (
    id INT AUTO_INCREMENT PRIMARY KEY,
    id_aluno VARCHAR(20),
    id_escola VARCHAR(20),
    sexo VARCHAR(10),
    etnia VARCHAR(50),
    nota_cn FLOAT,
    nota_ch FLOAT,
    nota_lc FLOAT,
    nota_mt FLOAT,
    nota_redacao FLOAT,
    nota_total FLOAT
);
''')

# Inserir dados
for _, row in df_pandas.iterrows():
    sql = """
        INSERT INTO Fato_Notas (id_aluno, id_escola, sexo, etnia, nota_cn, nota_ch, nota_lc, nota_mt, nota_redacao, nota_total)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    values = (row["id_aluno"], row["id_escola"], row["sexo"], row["etnia"], row["nota_cn"], row["nota_ch"], row["nota_lc"], row["nota_mt"], row["nota_redacao"], row["nota_total"])
    cursor.execute(sql, values)

conn.commit()
cursor.close()
conn.close()

print("ETL concluído!")
```

---

## 📌 Consultas SQL para Responder Perguntas (`scripts/queries.py`)
```python
import mysql.connector

def run_query(query):
    conn = mysql.connector.connect(host="localhost", user="root", password="root", database="enem")
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    return result

# Perguntas do desafio
queries = {
    "Escola com maior média": "SELECT id_escola, AVG(nota_total) FROM Fato_Notas GROUP BY id_escola ORDER BY AVG(nota_total) DESC LIMIT 1;",
    "Aluno com maior nota": "SELECT id_aluno, MAX(nota_total) FROM Fato_Notas;",
    "Média geral": "SELECT AVG(nota_total) FROM Fato_Notas;",
    "% de Ausentes": "SELECT (COUNT(*) - COUNT(nota_total)) / COUNT(*) * 100 FROM Fato_Notas;"
}

if __name__ == "__main__":
    for key, query in queries.items():
        print(f"{key}: {run_query(query)}")
```

---

## 📌 Como Rodar o Projeto
```bash
# Clonar o repositório
git clone https://github.com/seu-usuario/sx_negocios_etl.git
cd sx_negocios_etl

# Criar o banco de dados
python scripts/create_database.py

# Rodar o ETL
python scripts/etl.py

# Executar as consultas SQL
python scripts/queries.py
```

---

## 📌 Conclusão
✅ ETL completo usando PySpark.  
✅ Banco de dados MySQL rodando em Docker.  
✅ Dados do ENEM processados e carregados.  
✅ Consultas SQL para responder perguntas do desafio.  

Pronto para análise e visualização dos dados! 🚀
