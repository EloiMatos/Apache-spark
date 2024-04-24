# Reproduzindo o ambiente 

1. Subindo o container no docker com o jupyter 

docker run -p 8888:8888 quay.io/jupyter/pyspark-notebook

2. Faça a extração do dataset 

Rode o arquivo extraction.ipynb para importar o dataset que será análisado

3. Fazendo uso do pyspark

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("trabalho-engenharia-de-dados").config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.375").getOrCreate()
df = spark.read.csv("composin_PR_202311.csv")

4. Adicionar as Dependências do Apache Iceberg

Para usar o Apache Iceberg com o PySpark, você precisa adicionar as bibliotecas do Iceberg ao Spark. Você pode fazer isso configurando o SparkSession para incluir as dependências necessárias do Iceberg. Aqui está um exemplo de como fazer isso:

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("IcebergIntegration") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:0.14.0") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .enableHiveSupport() \
    .getOrCreate()


4. Execução do Jupyter Labs

Depois de configurar o ambiente, você pode iniciar o Jupyter Labs:

jupyter lab

5. Testando a Configuração

Dentro do Jupyter, você pode criar um notebook e escrever código para testar as configurações do Delta Lake e do Apache Iceberg, por exemplo, criando tabelas e executando queries.

6. Extração dos Dados

O processo de extração faz a requisição de dowload via a seguinte URL:
https://www.caixa.gov.br/Downloads/sinapi-a-partir-jul-2009-{uf}/SINAPI_ref_Insumos_Composicoes_{uf}_{anomes}_NaoDesonerado.zip

Os arquivos são baixados e extraídos para uma pasta temporária chamada EXTRACTION, após isso os dados são lidos e salvos em um csv chamado composin_uf_anomes.csv
