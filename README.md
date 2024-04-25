
# Reproduzindo o Ambiente

## 1. Subindo o Container com Jupyter no Docker

Para iniciar o ambiente de análise, execute o seguinte comando para iniciar um container Docker com o Jupyter e pyspark:

```bash
docker run -p 8888:8888 quay.io/jupyter/pyspark-notebook
```

Acesse o Jupyter Notebook pelo navegador utilizando o endereço `http://localhost:8888`.

---

## 2. Extração do Dataset

Arraste os arquivos do repositório em questão para dentro do ambiente Jupyter.

Antes de começar a análise, é necessário realizar a extração do dataset. Execute o notebook `working.ipynb` para importar o dataset que será utilizado na análise.

---

## 3. Utilizando o Delta Lake e Apache Iceberg com PySpark para Dataset

### 3.1 Delta Lake com PySpark

Para utilizar o Delta Lake com PySpark, siga os passos abaixo:

#### Listar os Containers Docker em Execução

```bash
docker ps
```

#### Acessar a Imagem com o Jupyter

```bash
docker exec -it [id_do_container] /bin/bash
```

#### Instalar o Pacote `delta-spark`

```bash
pip install delta-spark
```

#### Exemplo de Uso do PySpark com Delta Lake

```python
import pyspark
from delta import *

# Configuração do SparkSession com Delta Lake
builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

# Criando a sessão do Spark
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Leitura do arquivo CSV
df = spark.read.csv("composin_RS_202311.csv")

# Salvar como tabela Delta
df.write.format("delta").save("/home/jovyan/composin_RS_202311")
```
Para saber mais sobre o Delta Lake: https://delta.io/learn/getting-started/

### 3.2 Apache Iceberg com PySpark

Para utilizar o Apache Iceberg com PySpark, siga os passos abaixo:

```python
from pyspark.sql import SparkSession

# Configuração do SparkSession com Iceberg
spark = SparkSession.builder \
    .appName("IcebergExample") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .enableHiveSupport() \
    .getOrCreate()

# Leitura do arquivo CSV
df = spark.read.csv("composin_RS_202311.csv")

# Salvar como tabela Iceberg
df.write.format("iceberg").save("/home/jovyan/composin_RS_202311")
```

Para saber mais sobre Apache Iceberg: https://iceberg.apache.org/spark-quickstart/

---

## 4. Conclusão

Após seguir os passos acima, você estará pronto para iniciar a análise do dataset utilizando PySpark com Delta Lake ou Apache Iceberg.

---

## 5. Modelo ER e DDL para Tabelas Delta e Iceberg

**DDL (Data Definition Language)**:
```sql
CREATE TABLE composin (
    id INT,
    nome STRING,
    valor FLOAT
);
```

### Operações de INSERT, UPDATE e DELETE

**Delta Lake**:

- **INSERT**:
  Após criar a tabela Delta inicial, você pode inserir novos dados de outros datasets.
  ```python
  # Inserir dados de outro CSV
  df_novo = spark.read.csv("composin_SC_202311.csv")
  df_novo.write.format("delta").mode("append").save("/home/jovyan/composin")
  ```

- **UPDATE**:
  O Delta Lake suporta operações de update diretamente.
  ```python
  from delta.tables import *

  delta_table = DeltaTable.forPath(spark, "/home/jovyan/composin")
  delta_table.update(
      condition="id = 10",
      set={"valor": expr("valor + 100")}
  )
  ```

- **DELETE**:
  Deletar registros específicos.
  ```python
  delta_table.delete("id = 10")
  ```

**Apache Iceberg**:

- **INSERT**:
  Similar ao Delta Lake, adicionando dados com `append`.
  ```python
  df_novo = spark.read.csv("composin_SC_202311.csv")
  df_novo.write.format("iceberg").mode("append").save("/home/jovyan/composin")
  ```

- **UPDATE**:
  No Iceberg, operações de update são tratadas de maneira diferente e podem requerer sobreposição de dados ou reescrita.
  ```python
  df_atualizado = df.withColumn("valor", expr("valor + 100"))
  df_atualizado.write.format("iceberg").mode("overwrite").save("/home/jovyan/composin")
  ```

- **DELETE**:
  Iceberg também permite a exclusão direta, mas é mais comum usar filtros e sobreposições.
  ```python
  df_filtrado = df.filter("id != 10")
  df_filtrado.write.format("iceberg").mode("overwrite").save("/home/jovyan/composin")
  ```

Falta: 

• Descreva o cenário da(s) tabela(s) em um arquivo tipo notebook – modelo ER, imagens e 
códigos DDL - e da fonte de dados utilizada (preferência dados públicos).

• Explique e evidencie, com exemplos, os comandos de INSERT, UPDATE e DELETE nas tabelas 
Delta e Iceberg dentro do Apache Spark.

• Dentro do README, separe todos os cenários / exemplos do Delta Lake e Apache Iceberg.

