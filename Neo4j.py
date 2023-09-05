from pyspark.sql import SparkSession
#Gabriel Goulart Homem - 771011
#Guilherme Madureira Engler - 743545

#spark-submit --packages org.neo4j:neo4j-connector-apache-spark_2.12:5.0.1_for_spark_3 Neo4j.py

spark = SparkSession\
	.builder\
	.appName("PMD")\
	.config('spark.jars.packages', "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,neo4j-contrib:neo4j-connector-apache-spark_2.12:4.0.1_for_spark_3")\
	.getOrCreate()

df = spark.read.option("header",True).csv("pessoas.csv")
df = df.select("ID")
df.write.format("org.neo4j.spark.DataSource")\
	.option("url","bolt://localhost:7687")\
	.option("authentication.basic.username", "neo4j")\
	.option("authentication.basic.password", "12345678")\
	.option("labels",":Pessoa")\
	.option("node.keys","ID")\
	.mode("Overwrite")\
	.save()

for k in range(6):
	df = spark.read.option("multiline","true").json("splitNoticias/noticias"+str(k)+".json")
	df = df.select("id")
	df.write.format("org.neo4j.spark.DataSource")\
		.option("url","bolt://localhost:7687")\
		.option("authentication.basic.username", "neo4j")\
		.option("authentication.basic.password", "12345678")\
		.option("labels",":Noticia")\
		.option("node.keys","id")\
		.mode("Overwrite")\
		.save()

dfTransacoes = spark.read.format("csv").option("header", True).load("RelacionamentoNoticiasPessoas.csv")
dfTransacoes = dfTransacoes.select("ID0", "id1")
#dfTransacoes.show()

print("\n\n\n")

dfTransacoes.write.format("org.neo4j.spark.DataSource")\
	.mode("Append")\
    .option("url", "bolt://localhost:7687")\
    .option("authentication.basic.username", "neo4j")\
    .option("authentication.basic.password", "12345678")\
    .option("relationship", "LEU")\
    .option("relationship.save.strategy", "keys")\
    .option("relationship.source.labels", ":Pessoa")\
    .option("relationship.source.save.mode", "overwrite")\
    .option("relationship.source.node.keys", "ID0:ID")\
    .option("relationship.target.labels", ":Noticia")\
    .option("relationship.target.save.mode", "overwrite")\
    .option("relationship.target.node.keys", "id1:id")\
    .save()

print("acabou")