from pyspark.sql import SparkSession
#Gabriel Goulart Homem - 771011
#Guilherme Madureira Engler - 743545

appName = "PySpark MongoDB_Neo4j"
master = "local"
# Create Spark session
spark = SparkSession.builder \
	.appName(appName) \
	.master(master) \
	.config("spark.mongodb.read.connection.uri", "mongodb://localhost:27017") \
	.config("spark.mongodb.write.connection.uri", "mongodb://localhost:27017") \
	.config('spark.jars.packages', "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,\
		neo4j-contrib:neo4j-connector-apache-spark_2.12:4.0.1_for_spark_3")\
	.getOrCreate()

# #escreve Gabriel/Guilherme
#noticias
for k in range(6):
	noticias = spark.read.option("multiline","true").json("splitNoticias/noticias"+str(k)+".json")
	noticias.write.format('com.mongodb.spark.sql.DefaultSource').mode("append")\
		.option("uri", "mongodb://localhost:27017/g15_pmd.noticias").save()

#pessoas
pessoas = spark.read.option("header",True).csv("pessoas.csv")
pessoas.write.format('com.mongodb.spark.sql.DefaultSource').mode("append")\
	.option("uri", "mongodb://localhost:27017/g15_pmd.pessoas").save()

#le Gabriel/Guilherme	
print("Grupo 15 - Gabriel/Guilherme\n")
print("Noticias Inseridas no MongoDB:\n")
df = spark.read.format('com.mongodb.spark.sql.DefaultSource') \
	.option("uri", "mongodb://localhost:27017/g15_pmd.noticias").load()
#df.printSchema()
df.show()

print("Pessoas Inseridas no MongoDB:\n")
df = spark.read.format('com.mongodb.spark.sql.DefaultSource') \
	.option("uri", "mongodb://localhost:27017/g15_pmd.pessoas").load()
#df.printSchema()
df.show()
