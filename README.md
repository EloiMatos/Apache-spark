# Apache-spark

1. Instalação do pip (caso não esteja instalado):

curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
python get-pip.py

2. Instalação do PySpark e Jupyter Labs

pip install pyspark
pip install jupyterlab

3. Configuração do Delta Lake e Apache Iceberg

# Instalação do Delta Lake

pip install delta-spark

Para configurar o PySpark para usar o Delta Lake, você precisará ajustar a configuração do Spark Session:

from pyspark.sql import SparkSession

builder = SparkSession.builder.appName("DeltaLakeExample")
builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
builder.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = builder.getOrCreate()

# Configuração do Apache Iceberg

builder.config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:0.12.0")
builder.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
builder.config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
builder.config("spark.sql.catalog.local.type", "hadoop")
builder.config("spark.sql.catalog.local.warehouse", "/path/to/warehouse")

4. Execução do Jupyter Labs

Depois de configurar o ambiente, você pode iniciar o Jupyter Labs:

jupyter lab

5. Testando a Configuração

Dentro do Jupyter, você pode criar um notebook e escrever código para testar as configurações do Delta Lake e do Apache Iceberg, por exemplo, criando tabelas e executando queries.

6. Extração dos Dados

O processo de extração faz a requisição de dowload via a seguinte URL:
https://www.caixa.gov.br/Downloads/sinapi-a-partir-jul-2009-{uf}/SINAPI_ref_Insumos_Composicoes_{uf}_{anomes}_NaoDesonerado.zip

Os arquivos são baixados e extraídos para uma pasta temporária chamada EXTRACTION, após isso os dados são lidos e salvos em um csv chamado composin_uf_anomes.csv
