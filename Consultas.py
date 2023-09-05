from ast import Break
import re
from pyspark.sql import SparkSession
#Gabriel Goulart Homem - 771011
#Guilherme Madureira Engler - 743545

appName = "PySpark MongoDB"
master = "local"
# Create Spark session
spark = SparkSession.builder \
	.appName(appName) \
	.master(master) \
	.config("spark.mongodb.read.connection.uri", "mongodb://localhost:27017") \
	.config("spark.mongodb.write.connection.uri", "mongodb://localhost:27017") \
	.config('spark.jars.packages', "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,neo4j-contrib:neo4j-connector-apache-spark_2.12:4.0.1_for_spark_3")\
	.getOrCreate()

print("\n\n\n")
print("Escolha a proxima ação:\n")
print("1 Consulta 1: Recomendacao de noticias.")
print("2 Consulta 2: Pesquisa por noticias")
print("3 Para sair do programa.")
acao = input()
while acao != "3":
    match acao:
        case "1":
            #tem que trocas os nomes de tudo aqui nessa consulta, pq n sei como ela ta no banco
            PessoaId = input("Insira o ID da pessoa a qual sera feita a recomendacao:\n")
            df = spark.read.format("org.neo4j.spark.DataSource")\
                .option("url", "bolt://localhost:7687")\
                .option("authentication.basic.username", "neo4j")\
	            .option("authentication.basic.password", "12345678")\
                .option("query", 'match (pessoa1:Pessoa{ID:"%s"}) - [:LEU] - (noticia1:Noticia) - [:LEU] - (pessoa2:Pessoa) - [r:LEU] - (noticia2:Noticia)\
                                where not (pessoa1) --> (noticia2) and not pessoa1 = pessoa2 and not noticia1 = noticia2\
                                with noticia2, count(distinct(r)) as contador\
                                order by contador DESC\
                                limit 1\
                                return noticia2, contador'%(PessoaId))\
                .load()
            
            df.show()
            if(df.head() != None):
                id = df.head().noticia2.id
                print(df.head())
                # print(df.head().noticia2.id)

                df = spark.read.format('com.mongodb.spark.sql.DefaultSource') \
                    .option("uri", "mongodb://localhost:27017/g15_pmd.noticias")\
                    .option("pipeline","[{$match: {id:%s}}]"%(id))\
                    .load()
                #df.printSchema()
                df.show()
            else:
                print("sem notícias para recomendar para essa pessoa!")
        case "2":
            #Pesquisa de noticias baseado em autores, keywords(titulo e texto da noticia) e datas(inicio e fim) e categorias 
            filtro = "[{$match: {"

            autor = input('Busca por autor?(use "none" para nao buscar por autor)\n')
            if (autor != "none"):
                filtro = filtro + "authors:{$regex :'%s'}" % (autor)

            titulo = input('Busca por titulo?(use "none" para nao buscar por titulo)\n')
            if (titulo != "none"):
                filtro = filtro + "title:{$regex :'%s'}" % (titulo)
                
            texto = input('Busca por algo no texto?(use "none" para nao buscar por texto)\n')
            if (texto != "none"):
                filtro = filtro + "text:{$regex :'%s'}" % (texto)

            saida = 0
            while saida == 0:
                data = input('Busca por intervalo de tempo?(formatos:dataInicio>data<dataFim:"AAAA-MM-DD AAAA-MM-DD", data<dataFim"none AAAA-MM-DD"'\
                        +',dataIni>data"AAAA-MM-DD none" e sem data "none none")\n')
                if len(data.split()) == 2:
                    dataIni, dataFim = data.split()
                    if ((re.fullmatch(r'\d{4}-\d{2}-\d{2}',dataIni) is not None or dataIni == "none") \
                        and (re.fullmatch(r'\d{4}-\d{2}-\d{2}', dataFim) is not None or dataFim == "none")):
                        if (dataIni != "none" and dataFim != "none"):
                            filtro = filtro + 'date:{$gte:"%s",$lte:"%s"}' % (dataIni,dataFim)
                        elif (dataIni == "none" and dataFim != "none"):
                            filtro = filtro + 'date:{$lte:"%s"}' % (dataFim)   
                        elif (dataIni != "none" and dataFim == "none"):
                            filtro = filtro + 'date:{$gte:"%s"}' % (dataIni)            
                        # if (dataIni != "none" and dataFim != "none"):
                        #     filtro = filtro + 'date:{$gte:ISODate("%s"),$lte:ISODate("%s")}' % (dataIni,dataFim)
                        # elif (dataIni == "none" and dataFim != "none"):
                        #     filtro = filtro + 'date:{$lte:ISODate("%s")}' % (dataFim)   
                        # elif (dataIni != "none" and dataFim == "none"):
                        #     filtro = filtro + 'date:{$gte:ISODate("%s")}' % (dataIni)            
                        saida = 1
                    else:
                        print("Entrada no formato incorreto")
                else:
                    print("Esperado duas entradas")

            categoria = input('Busca por uma categoria?(use "none" para nao buscar por categoria)\n')
            if (categoria != "none"):
                filtro = filtro + "category:{$regex :'%s'}" % (categoria)

            filtro = filtro + "}}]"


            df = spark.read.format('com.mongodb.spark.sql.DefaultSource') \
                .option("uri", "mongodb://localhost:27017/g15_pmd.noticias")\
                .option("pipeline",filtro)\
                .load()
            #df.printSchema()
            df.show()
        case _:
            print("Entrada invalida\n")
    
    print("\n\n\n")
    print("Escolha a proxima ação:\n")
    print("1 Consulta 1: Recomendacao de noticias.")
    print("2 Consulta 2: Pesquisa por noticias")
    print("3 Para sair do programa: \n")
    acao = input()