# Databricks notebook source
# MAGIC %md
# MAGIC ## MVP Engenharia de Dados

# COMMAND ----------

# MAGIC %md
# MAGIC ### Objetivo
# MAGIC
# MAGIC O mercado de apostas esportivas tem crescido consideravelmente. Segundo dados da Anbima (Associação Brasileira das Entidades dos Mercados Financeiro e de Capitais), 14% dos brasileiros fizeram ao menos uma aposta esportiva no ano de 2023. Este número supera com folga o de investidores de títulos privados, fundos de investimento e títulos públicos (respectivamente, por 5%, 4% e 2% dos brasileiros).
# MAGIC Dentro deste mercado, a categoria que mais movimenta recursos e apostadores é a de futebol.
# MAGIC A proposta deste trabalho é utilizar uma base de dados com dados de todas as partidas das cinco principais ligas de futebol europeias (alemã, espanhola, francesa, inglesa e italiana), na temporada de 2023-24. Além dos placares de cada partida, esta base também contém dados da partida, como chutes a gol e escanteios.
# MAGIC A ideia seria cruzar estes dados com as cotações (“odds”) que as principais empresas de apostas esportivas do mercado apontavam antes do início de cada partida.  
# MAGIC A princípio, iremos nos concentrar apenas nas apostas de probabilidade do resultado final de cada partida, mas podemos tentar explorar também outras modalidades no desenvolvimento deste trabalho.
# MAGIC Algumas perguntas que este trabalho se propõe a responder:
# MAGIC - Qual o índice de acerto das previsões das casas de aposta para o resultado da partida?
# MAGIC - Alguma liga segue mais o padrão destas previsões? Alguma está mais propensa a “zebras”?
# MAGIC - Em algum período da temporada, as probabilidades de apostas se confirmam mais ou menos do que em outros períodos?
# MAGIC - Há alguma distorção entre as probabilidades que cada empresa trabalha antes das partidas, ou elas seguem o mesmo padrão?
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##Plataforma
# MAGIC
# MAGIC Para a elaboração deste MVP, seguimos a orientação dos professores da disciplina e utilizamos o Databricks Community Edition. Chegamos a prospectar a utilização de ferramentas AWS S3, como o Glue Studio e o Redshift, mas desistimos após o feedback obtido nos encontros para tirar dúvidas. Para criar, tratar a base e fazer as consultas, foram utilizadas as linguagens PySpark e SQL.
# MAGIC Os datasets e notebooks criados estão disponibilizados no GitHUb: https://github.com/diogomattos1/mvp-engenharia-dados
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##Detalhamento
# MAGIC
# MAGIC ###Busca pelos dados
# MAGIC
# MAGIC Para os dados deste problema, foi utilizada um dataset obtido no site Kaggle (https://www.kaggle.com/datasets/audreyhengruizhang/european-soccer-data), de autoria de Audrey Hengrui Zhang. O autor utilizou o site https://www.football-data.co.uk/ para obter os dados que compõem este dataset.
# MAGIC Este dataset possui todos os resultados das 5 principais ligas de futebol da Europa (Alemanha, Espanha, França, Inglaterra e Itália).

# COMMAND ----------

# MAGIC %md
# MAGIC ###Coleta
# MAGIC Utilizaremos apenas os dados do dataset disponível no Kaggle, pois julgamos que ele apresenta bastante informação em suas 104 colunas para responder as perguntas propostas. 
# MAGIC Cogitamos buscar dados sobre volume de apostas e valor financeiro envolvido nelas, para cada uma das partidas, o que tornaria a base de dados mais rica e que até mudaria o enfoque das perguntas inicialmente feitas. Mas estes não são disponibilizados pelas empresas, provavelmente por ser uma informação estratégica.
# MAGIC Desta forma, vamos à atividade de coleta dos dados:
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --Limpando os bancos de dados antes de executar o trabalho, caso seja necessário
# MAGIC DROP TABLE IF EXISTS bronze.europa;
# MAGIC DROP TABLE IF EXISTS silver.europa;

# COMMAND ----------

#Limpando arquivos antes de executar o trabalho, caso seja necessário
dbutils.fs.rm("dbfs:/FileStore/Bundesliga23.csv", recurse=True)
dbutils.fs.rm("dbfs:/FileStore/SerieA23.csv", recurse=True)
dbutils.fs.rm("dbfs:/FileStore/Ligue1.csv", recurse=True)
dbutils.fs.rm("dbfs:/FileStore/PL23.csv", recurse=True)
dbutils.fs.rm("dbfs:/FileStore/LaLiga23.csv", recurse=True)
dbutils.fs.rm("dbfs:/FileStore/notes.txt", recurse=True)
dbutils.fs.rm("dbfs:/FileStore/tables/archive.zip", recurse=True)




# COMMAND ----------

dbutils.fs.rm("dbfs:/user/hive/warehouse/bronze.db", recurse=True)
dbutils.fs.rm("dbfs:/user/hive/warehouse/silver.db", recurse=True)

# COMMAND ----------

#Instalando biblioteca para conexão com Kaggle

%pip install kaggle
%pip install snowflake-connector-python

# COMMAND ----------

#Reiniciando o Python
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %sh
# MAGIC #Baixando o arquivo com o dataset
# MAGIC kaggle datasets download -d audreyhengruizhang/european-soccer-data

# COMMAND ----------

#Listando o arquivo baixado
dbutils.fs.ls('file:/databricks/driver')

# COMMAND ----------

# MAGIC %sh
# MAGIC #Descompactando o arquivo
# MAGIC unzip /databricks/driver/european-soccer-data.zip

# COMMAND ----------

#Movendo os arquivos para o FileStore
dbutils.fs.mv('file:/databricks/driver/Bundesliga23.csv', 'dbfs:/FileStore/', recurse=True)
dbutils.fs.mv('file:/databricks/driver/SerieA23.csv', 'dbfs:/FileStore/', recurse=True)
dbutils.fs.mv('file:/databricks/driver/LaLiga23.csv', 'dbfs:/FileStore/', recurse=True)
dbutils.fs.mv('file:/databricks/driver/Ligue1.csv', 'dbfs:/FileStore/', recurse=True)
dbutils.fs.mv('file:/databricks/driver/PL23.csv', 'dbfs:/FileStore/', recurse=True)
dbutils.fs.mv('file:/databricks/driver/notes.txt', 'dbfs:/FileStore/', recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Arquivos disponíveis no FileStore:

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/")

# COMMAND ----------

# MAGIC %md
# MAGIC  
# MAGIC ![test image](files/tables/mvp_1.jpg)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##Modelagem
# MAGIC Para este trabalho, o modelo que trabalharemos será o flat. O próprio dataset que foi escolhido já está nesta estrutura, desta forma será necessário apenas fazer alguns tratamentos posteriores.
# MAGIC O dataset traz em um arquivo .txt a descrição detalhada de cada campo das tabelas, que utilizaremos como catálogo de dados de referência para o MVP:
# MAGIC

# COMMAND ----------

#Exibindo o catálogo de dados
file_path = "dbfs:/FileStore/notes.txt"
content = dbutils.fs.head(file_path)
print(content)

# COMMAND ----------

# MAGIC %md
# MAGIC Além dos resultados e estatísticas da partida, o arquivo traz também as probabilidades de vitória de cada time ou empate nas partidas e outros tipos de apostas, como por exemplo:
# MAGIC - over/under 2,5 gols: é possível apostar se uma determinada partida terá um placar final de até 2 gols; ou de 3 ou mais gols.
# MAGIC - asian handicap: As apostas em handicap asiático são uma forma de apostar no futebol em que os times mais fortes já começam a partida “prejudicados”, de modo que devam vencer por placares maiores para ter sua aposta considerada vencedora.
# MAGIC
# MAGIC Não vamos utilizar todos estes dados para o MVP, mas estas últimas categorias permitem realizar mais perguntas além das previamente planejadas.
# MAGIC
# MAGIC Observação: verificamos que o arquivo do catálogo de dados apresenta mais informações do que os arquivos .csv. Iremos tratar isso posteriormente, na etapa de ETL.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##Carga
# MAGIC Para realizar o processo de extração, transformação e carga (ETL), inicialmente, vamos avaliar os arquivos que coletamos e já armazenamos no FileSystem:

# COMMAND ----------

# DBTITLE 1,e,
dfbl = spark.read.format('csv').option('header', 'true').load('/FileStore/Bundesliga23.csv')
dfsa = spark.read.format('csv').option('header', 'true').load('/FileStore/SerieA23.csv')
dfl1 = spark.read.format('csv').option('header', 'true').load('/FileStore/Ligue1.csv')
dfll = spark.read.format('csv').option('header', 'true').load('/FileStore/LaLiga23.csv')
dfpl = spark.read.format('csv').option('header', 'true').load('/FileStore/PL23.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC Ao analisar os arquivos, foi observado que o arquivo PL23.csv possui uma coluna a mais que os demais. Analisando os arquivos percebemos que a coluna a mais que o arquivo possuía era a “Referee” (árbitro). Esta não será necessária para as perguntas do nosso MVP.

# COMMAND ----------

dfpl = spark.read.format('csv').option('header', 'true').load('/FileStore/PL23.csv').drop('Referee')

# COMMAND ----------

# MAGIC %md
# MAGIC Outro ponto que precisará ser trabalhado são os tipos de dados. Todas as colunas dos arquivos .csv estão no formato string. Vamos converter os campos dos arquivos em formatos que possam ser trabalhados posteriormente (data, hora, int e double). E depois vamos fazer uma junção dos arquivos correspondentes a cada liga em uma única visão (o nosso modelo flat).

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

# Inicialize a sessão do Spark
spark = SparkSession.builder.getOrCreate()

# Carregue o arquivo CSV como DataFrame
dfbl = spark.read.csv("/FileStore/Bundesliga23.csv", header=True, inferSchema=True)
dfsa = spark.read.csv("/FileStore/SerieA23.csv", header=True, inferSchema=True)
dfl1 = spark.read.csv("/FileStore/Ligue1.csv", header=True, inferSchema=True)
dfll = spark.read.csv("/FileStore/LaLiga23.csv", header=True, inferSchema=True)
dfpl = spark.read.csv("/FileStore/PL23.csv", header=True, inferSchema=True).drop('Referee')

# Converta a coluna "date" para o tipo "date"
dfbl = dfbl.withColumn("Date", to_date(col("Date"), "dd-MM-yyyy"))
dfsa = dfsa.withColumn("Date", to_date(col("Date"), "dd-MM-yyyy"))
dfl1 = dfl1.withColumn("Date", to_date(col("Date"), "dd-MM-yyyy"))
dfll = dfll.withColumn("Date", to_date(col("Date"), "dd-MM-yyyy"))
dfpl = dfpl.withColumn("Date", to_date(col("Date"), "dd-MM-yyyy"))

# Unir as 5 tabelas
df_final = dfbl.union(dfl1).union(dfll).union(dfsa).union(dfpl)
# Exibir
display(df_final)

# COMMAND ----------

# MAGIC %md
# MAGIC Também foi observado que algumas colunas dos arquivos .csv apresentavam nomes que poderiam vir a causar erro nas etapas posteriores, seja porque são palavras que já pertencem à linguagem SQL ou por apresentarem espaços ou caracteres inválidos.

# COMMAND ----------

# Renomeie as colunas usando a função withColumnRenamed
df_final = df_final.withColumnRenamed('Div', 'League')
df_final = df_final.withColumnRenamed('Date', 'DateMatch')
df_final = df_final.withColumnRenamed('Time', 'TimeMatch')
df_final = df_final.withColumnRenamed('AS', 'ASS')
df_final = df_final.withColumnRenamed('FTHG and HG', 'FTHGandHG')
df_final = df_final.withColumnRenamed('FTAG and AG', 'FTAGandAG')
df_final = df_final.withColumnRenamed('FTR and Res', 'FTRandRes')
df_final = df_final.withColumnRenamed('B365>2.5', 'B365O25')
df_final = df_final.withColumnRenamed('B365<2.5', 'B365U25')
df_final = df_final.withColumnRenamed('P>2.5', 'PO25')
df_final = df_final.withColumnRenamed('P<2.5', 'PU25')
df_final = df_final.withColumnRenamed('Max>2.5', 'MaxO25')
df_final = df_final.withColumnRenamed('Max<2.5', 'MaxU25')
df_final = df_final.withColumnRenamed('Avg>2.5', 'AvgO25')
df_final = df_final.withColumnRenamed('Avg<2.5', 'AvgU25')

# Exibir DataFrame com a coluna renomeada
display(df_final)

# COMMAND ----------

# MAGIC %md
# MAGIC Já tendo os dados e no formato desejado, vamos providenciar a criação do banco de dados “bronze”, para salvar os dados dos 5 arquivos em uma única tabela “europa”. Nela, os dados serão salvos já no formato adequado para o trabalho (seja string, date, timestamp, int ou double).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS bronze

# COMMAND ----------

dbutils.fs.rm('dbfs:/user/hive/warehouse/bronze.db/europa',True) 
df_final.write.format("delta").mode("append").saveAsTable("bronze.europa")

# COMMAND ----------

# MAGIC %md
# MAGIC Podemos observar que o banco foi criado. Entretanto, o schema não possui a descrição de cada coluna do banco. Vamos providenciar o cadastro dos metadados da tabela "europa".

# COMMAND ----------

#exibindo schema com metadados
spark.sql("DESCRIBE EXTENDED bronze.europa").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC  
# MAGIC ![test image](files/tables/mvp_2.jpg)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Incluindo metadados no schema
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	League 	League 	STRING	COMMENT 'League Division';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	DateMatch 	DateMatch 	DATE	COMMENT 'Match Date (dd/mm/yy)';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	TimeMatch 	TimeMatch 	TIMESTAMP	COMMENT 'Time of match kick off';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	HomeTeam 	HomeTeam 	STRING	COMMENT 'Home Team';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	AwayTeam 	AwayTeam 	STRING	COMMENT 'Away Team';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	FTHG	FTHG	INT	COMMENT 'Full Time Home Team Goals';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	FTAG	FTAG	INT	COMMENT 'Full Time Away Team Goals';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	FTR	FTR	STRING	COMMENT 'Full Time Result (H Home Win, D Draw, A Away Win)';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	HTHG 	HTHG 	INT	COMMENT 'Half Time Home Team Goals';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	HTAG 	HTAG 	INT	COMMENT 'Half Time Away Team Goals';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	HTR 	HTR 	STRING	COMMENT 'Half Time Result (H Home Win, D Draw, A Away Win)';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	HS 	HS 	INT	COMMENT 'Home Team Shots';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	ASS 	ASS 	INT	COMMENT 'Away Team Shots';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	HST 	HST 	INT	COMMENT 'Home Team Shots on Target';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	AST 	AST 	INT	COMMENT 'Away Team Shots on Target';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	HC 	HC 	INT	COMMENT 'Home Team Corners';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	AC 	AC 	INT	COMMENT 'Away Team Corners';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	HF 	HF 	INT	COMMENT 'Home Team Fouls Committed';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	AF 	AF 	INT	COMMENT 'Away Team Fouls Committed';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	HY 	HY 	INT	COMMENT 'Home Team Yellow Cards';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	AY 	AY 	INT	COMMENT 'Away Team Yellow Cards';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	HR 	HR 	INT	COMMENT 'Home Team Red Cards';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	AR 	AR 	INT	COMMENT 'Away Team Red Cards';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	B365H 	B365H 	DOUBLE	COMMENT 'Bet365 home win odds';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	B365D 	B365D 	DOUBLE	COMMENT 'Bet365 draw odds';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	B365A 	B365A 	DOUBLE	COMMENT 'Bet365 away win odds';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	BWH 	BWH 	DOUBLE	COMMENT 'Bet&Win home win odds';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	BWD 	BWD 	DOUBLE	COMMENT 'Bet&Win draw odds';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	BWA 	BWA 	DOUBLE	COMMENT 'Bet&Win away win odds';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	IWH 	IWH 	DOUBLE	COMMENT 'Interwetten home win odds';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	IWD 	IWD 	DOUBLE	COMMENT 'Interwetten draw odds';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	IWA 	IWA 	DOUBLE	COMMENT 'Interwetten away win odds';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	PSH	PSH	DOUBLE	COMMENT 'Pinnacle home win odds';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	PSD	PSD	DOUBLE	COMMENT 'Pinnacle draw odds';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	PSA	PSA	DOUBLE	COMMENT 'Pinnacle away win odds';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	VCH 	VCH 	DOUBLE	COMMENT 'VC Bet home win odds';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	VCD 	VCD 	DOUBLE	COMMENT 'VC Bet draw odds';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	VCA 	VCA 	DOUBLE	COMMENT 'VC Bet away win odds';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	WHH 	WHH 	DOUBLE	COMMENT 'William Hill home win odds';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	WHD 	WHD 	DOUBLE	COMMENT 'William Hill draw odds';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	WHA 	WHA 	DOUBLE	COMMENT 'William Hill away win odds';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	MaxH 	MaxH 	DOUBLE	COMMENT 'Market maximum home win odds';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	MaxD 	MaxD 	DOUBLE	COMMENT 'Market maximum draw win odds';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	MaxA 	MaxA 	DOUBLE	COMMENT 'Market maximum away win odds';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	AvgH 	AvgH 	DOUBLE	COMMENT 'Market average home win odds';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	AvgD 	AvgD 	DOUBLE	COMMENT 'Market average draw win odds';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	AvgA 	AvgA 	DOUBLE	COMMENT 'Market average away win odds';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	AHh 	AHh 	DOUBLE	COMMENT 'Market size of handicap (home team) (since 2019/2020)';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	B365AHH 	B365AHH 	DOUBLE	COMMENT 'Bet365 Asian handicap home team odds';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	B365AHA 	B365AHA 	DOUBLE	COMMENT 'Bet365 Asian handicap away team odds';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	PAHH 	PAHH 	DOUBLE	COMMENT 'Pinnacle Asian handicap home team odds';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	PAHA 	PAHA 	DOUBLE	COMMENT 'Pinnacle Asian handicap away team odds';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	MaxAHH 	MaxAHH 	DOUBLE	COMMENT 'Market maximum Asian handicap home team odds';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	MaxAHA 	MaxAHA 	DOUBLE	COMMENT '	" Market maximum Asian handicap away team odds	"';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	AvgAHH 	AvgAHH 	DOUBLE	COMMENT 'Market average Asian handicap home team odds';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	AvgAHA 	AvgAHA 	DOUBLE	COMMENT 'Market average Asian handicap away team odds';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	B365O25 	B365O25 	DOUBLE	COMMENT 'Bet365 over 2.5 goals';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	B365U25 	B365U25 	DOUBLE	COMMENT 'Bet365 under 2.5 goals';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	PO25 	PO25 	DOUBLE	COMMENT 'Pinnacle over 2.5 goals';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	PU25 	PU25 	DOUBLE	COMMENT 'Pinnacle under 2.5 goals';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	MaxO25 	MaxO25 	DOUBLE	COMMENT 'Market maximum over 2.5 goals';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	MaxU25 	MaxU25 	DOUBLE	COMMENT 'Market maximum under 2.5 goals';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	AvgO25 	AvgO25 	DOUBLE	COMMENT 'Market average over 2.5 goals';
# MAGIC ALTER TABLE bronze.europa CHANGE COLUMN 	AvgU25 	AvgU25 	DOUBLE	COMMENT 'Market average under 2.5 goals';

# COMMAND ----------

# MAGIC %md
# MAGIC Agora sim a tabela "europa" está com os metadados do catálogo de informações. 

# COMMAND ----------

#exibindo schema com metadados
spark.sql("DESCRIBE EXTENDED bronze.europa").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC  
# MAGIC ![test image](files/tables/mvp_3-1.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC Por fim, apenas para fins didáticos neste MVP, vamos transformar os valores da nova coluna “League”, que no dataset original continha códigos, para o nome do país de cada liga, para facilitar a visualização dos resultados das consultas.

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE bronze.europa 
# MAGIC SET League = 'Inglaterra' 
# MAGIC WHERE League = 'E0';
# MAGIC
# MAGIC UPDATE bronze.europa 
# MAGIC SET League = 'Itália' 
# MAGIC WHERE League = 'I1';
# MAGIC
# MAGIC UPDATE bronze.europa 
# MAGIC SET League = 'Espanha' 
# MAGIC WHERE League = 'SP1';
# MAGIC
# MAGIC UPDATE bronze.europa 
# MAGIC SET League = 'Alemanha' 
# MAGIC WHERE League = 'D1';
# MAGIC
# MAGIC UPDATE bronze.europa 
# MAGIC SET League = 'França' 
# MAGIC WHERE League = 'F1';

# COMMAND ----------

# MAGIC %md
# MAGIC ##Análise
# MAGIC ###Qualidade dos Dados
# MAGIC Alguns problemas encontrados no dataset foram tratados, conforme mostrado em sessões anteriores: exclusão de uma coluna (“referee”), transformação dos tipos de dados, inserção de metadados, alteração do nome das colunas. Também foi verificado que nem todas as informações contidas no arquivo de catálogo de dados não constavam na base, mas essas ausências não trouxeram perda da qualidade para responder as perguntas propostas.
# MAGIC
# MAGIC Continuando a análise das informações contidas no banco de dados, concluímos que as colunas que trazem os valores máximo e média das “odds” de cada partida não correspondem aos valores do dataset.
# MAGIC
# MAGIC Aqui o exemplo de uma partida entre as equipes Lyon e Le Havre, onde podemos observar uma diferença entre o maior valor para a vitória do time da casa entre todas as 6 casas disponíveis (2,71) é diferente do maior valor (2,75) que o dataset trouxe consolidado na coluna MaxH, e que segundo o catálogo de dados, corresponderia ao “Market maximum home win odds”. Note que também há diferença para a cotação máxima para empate (4,23 e 4,33) e para vitória do time visitante (4,82 e 5).
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --Comparando o maior valor das colunas de cada cotação com o maior valor consolidado nas colunas de máximo
# MAGIC SELECT 
# MAGIC     GREATEST(B365H, BWH, IWH, PSH, WHH, VCH) AS maior_valor_H,
# MAGIC     MaxH,
# MAGIC     GREATEST(B365D, BWD, IWD, PSD, WHD, VCD) AS maior_valor_D,
# MAGIC     MaxD,
# MAGIC     GREATEST(B365A, BWA, IWA, PSA, WHA, VCA) AS maior_valor_A,
# MAGIC     MaxA
# MAGIC FROM
# MAGIC     bronze.europa
# MAGIC WHERE
# MAGIC     HomeTeam = "Lyon" 
# MAGIC     AND AwayTeam = "Le Havre";

# COMMAND ----------

# MAGIC %md
# MAGIC Podemos ver que o mesmo problema ocorre quando comparamos as médias:

# COMMAND ----------

# MAGIC %sql
# MAGIC --Comparando a média das colunas de cada cotação com as colunas de média consolidada
# MAGIC SELECT 
# MAGIC     ROUND((B365H + BWH + IWH + PSH + WHH + VCH) / 6, 2) AS media_H,
# MAGIC     AvgH,
# MAGIC     ROUND((B365D + BWD + IWD + PSD + WHD + VCD) / 6, 2) AS media_D,
# MAGIC     AvgD,
# MAGIC     ROUND((B365H + BWA + IWA + PSA + WHA + VCA) / 6, 2) AS media_A,
# MAGIC     AvgA
# MAGIC FROM
# MAGIC     bronze.europa
# MAGIC WHERE
# MAGIC     HomeTeam = "Lyon" 
# MAGIC     AND AwayTeam = "Le Havre";

# COMMAND ----------

# MAGIC %md
# MAGIC Os arquivos apresentam as cotações de 6 casas de apostas, enquanto o arquivo de catálogo de informações mostra dados de 12 casas. Acreditamos que os máximos e médias tenham sido calculados em cima da base completa com estas 12 casas, mas não podemos garantir essa hipótese. O mesmo problema com máximos e médias ocorre para as cotações de “over/under” e “asian handicap” (mas estas, a princípio, não serão necessárias para responder às perguntas, por isso não iremos ajustar neste MVP).
# MAGIC
# MAGIC Antes de iniciar o tratamento destas colunas, vamos criar a camada silver do banco de dados para trabalhar.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --Criando o banco de dados silver
# MAGIC CREATE DATABASE IF NOT EXISTS silver

# COMMAND ----------

# Define o banco de dados "bronze" como o banco de dados padrão
spark.sql("USE bronze")

# Copia a tabela "europa" para o banco de dados "silver"
spark.sql("CREATE TABLE silver.europa USING DELTA AS SELECT * FROM bronze.europa")

# COMMAND ----------

# MAGIC %sql
# MAGIC --Conferindo se a criação foi correta
# MAGIC SELECT * FROM silver.europa

# COMMAND ----------

# MAGIC %md
# MAGIC Agora vamos corrigir as informações de máximos e médias. Para isso, a sugestão do trabalho será criar colunas para registrar os valores de máximo e média que sejam necessários para responder às perguntas propostas, considerando apenas das empresas cujos dados constam no dataset.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE silver.europa ADD COLUMN MaiorValorH DOUBLE;
# MAGIC ALTER TABLE silver.europa ADD COLUMN MaiorValorD DOUBLE;
# MAGIC ALTER TABLE silver.europa ADD COLUMN MaiorValorA DOUBLE;
# MAGIC
# MAGIC UPDATE silver.europa
# MAGIC SET MaiorValorH = GREATEST(B365H, BWH, IWH, PSH, WHH, VCH),
# MAGIC     MaiorValorD = GREATEST(B365D, BWD, IWD, PSD, WHD, VCD),
# MAGIC     MaiorValorA = GREATEST(B365A, BWA, IWA, PSA, WHA, VCA);

# COMMAND ----------

# MAGIC %md
# MAGIC Como foi observado que, para algumas partidas, o valor das cotações de alguma casa de apostas poderia estar indisponível (estando assim “null” no banco de dados), usamos um tratamento com a função COALESCE para que as médias desconsiderassem esses casos.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE silver.europa ADD COLUMN MediaH DOUBLE;
# MAGIC ALTER TABLE silver.europa ADD COLUMN MediaD DOUBLE;
# MAGIC ALTER TABLE silver.europa ADD COLUMN MediaA DOUBLE;
# MAGIC
# MAGIC UPDATE silver.europa
# MAGIC SET MediaH = ROUND((COALESCE(B365H, 0) + COALESCE(BWH, 0) + COALESCE(IWH, 0) + COALESCE(PSH, 0) + COALESCE(WHH, 0) + COALESCE(VCH, 0)) / 
# MAGIC                   (CASE WHEN B365H IS NOT NULL THEN 1 ELSE 0 END +
# MAGIC                    CASE WHEN BWH IS NOT NULL THEN 1 ELSE 0 END +
# MAGIC                    CASE WHEN IWH IS NOT NULL THEN 1 ELSE 0 END +
# MAGIC                    CASE WHEN PSH IS NOT NULL THEN 1 ELSE 0 END +
# MAGIC                    CASE WHEN WHH IS NOT NULL THEN 1 ELSE 0 END +
# MAGIC                    CASE WHEN VCH IS NOT NULL THEN 1 ELSE 0 END), 2),
# MAGIC     MediaD = ROUND((COALESCE(B365D, 0) + COALESCE(BWD, 0) + COALESCE(IWD, 0) + COALESCE(PSD, 0) + COALESCE(WHD, 0) + COALESCE(VCD, 0)) / 
# MAGIC                   (CASE WHEN B365D IS NOT NULL THEN 1 ELSE 0 END +
# MAGIC                    CASE WHEN BWD IS NOT NULL THEN 1 ELSE 0 END +
# MAGIC                    CASE WHEN IWD IS NOT NULL THEN 1 ELSE 0 END +
# MAGIC                    CASE WHEN PSD IS NOT NULL THEN 1 ELSE 0 END +
# MAGIC                    CASE WHEN WHD IS NOT NULL THEN 1 ELSE 0 END +
# MAGIC                    CASE WHEN VCD IS NOT NULL THEN 1 ELSE 0 END), 2),
# MAGIC     MediaA = ROUND((COALESCE(B365A, 0) + COALESCE(BWA, 0) + COALESCE(IWA, 0) + COALESCE(PSA, 0) + COALESCE(WHA, 0) + COALESCE(VCA, 0)) / 
# MAGIC                   (CASE WHEN B365A IS NOT NULL THEN 1 ELSE 0 END +
# MAGIC                    CASE WHEN BWA IS NOT NULL THEN 1 ELSE 0 END +
# MAGIC                    CASE WHEN IWA IS NOT NULL THEN 1 ELSE 0 END +
# MAGIC                    CASE WHEN PSA IS NOT NULL THEN 1 ELSE 0 END +
# MAGIC                    CASE WHEN WHA IS NOT NULL THEN 1 ELSE 0 END +
# MAGIC                    CASE WHEN VCA IS NOT NULL THEN 1 ELSE 0 END), 2);
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##Solução do Problema
# MAGIC Vamos repetir aqui as perguntas inicialmente feitas, e para responde-las, precisaremos dar algumas explicações e possivelmente realizar novas transformações de dados antes de buscar a resposta adequada.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### - Qual o índice de acerto das previsões das casas de aposta para o resultado da partida?
# MAGIC À medida que estudamos como funcionam as apostas, chegamos à conclusão de que as cotações das apostas não fornecem subsídios para tentar prever o placar final das partidas. Então vamos trabalhar apenas com o resultado final: se a partida terá como vencedora a equipe da casa, a visitante ou terminará em empate.
# MAGIC Para tentar responder à esta pergunta, pensamos em duas linhas: uma com valores absolutos e outra com relativos.
# MAGIC Vamos selecionar uma partida do dataset para exemplificar as duas abordagens:
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --placar e cotações médias da partida entre Barcelona e Girona
# MAGIC SELECT HomeTeam, AwayTeam, FTHG, FTAG, FTR, MediaH, MediaD, MediaA
# MAGIC FROM silver.europa
# MAGIC WHERE HomeTeam = "Barcelona"
# MAGIC AND AwayTeam = "Girona"

# COMMAND ----------

# MAGIC %md
# MAGIC Para esta partida, as melhores “odds” (1,51) indicavam o time da casa, o Barcelona, como vencedor da partida. Como o placar final foi 4x2 para o visitante, o Girona, podemos considerar em uma análise binária que as casas de apostas simplesmente erraram o vencedor. Esta seria a abordagem absoluta.
# MAGIC
# MAGIC Porém, vamos ver como as “odds” são calculadas:
# MAGIC
# MAGIC Quando, no exemplo acima, a casa indica que a cotação para o Barcelona vencer é de 1,51, podemos transpor isso para um valor percentual:
# MAGIC
# MAGIC     
# MAGIC     Vitória do Barcelona: (1/1,51)* 100 = 66,23%
# MAGIC
# MAGIC Logo, o Barcelona teria 66,23% de chances de vencer a partida.
# MAGIC
# MAGIC Analogamente, as probabilidades de empate e de vitória do Girona seriam de, respectivamente, 21,1% e 17,79%:
# MAGIC
# MAGIC     
# MAGIC     Empate: (1/4,74)* 100 = 21,1%
# MAGIC   
# MAGIC     Vitória do Girona: (1/5,62)* 100 = 17,79%
# MAGIC
# MAGIC Podemos observar que, neste exemplo, o somatório ultrapassa os 100% (no caso, 105,12%). Isso é o que chamam, no mercado de apostas, de “margem”. Na prática, seria uma espécie de comissão que as casas levam.
# MAGIC
# MAGIC Se normalizarmos este valor, para que o total seja de 100%, podemos considerar que o percentual de probabilidade de cada resultado seria:
# MAGIC
# MAGIC   Vitória do Barcelona: 63%
# MAGIC   Empate: 20,08%
# MAGIC   Vitória do Girona: 16,92%
# MAGIC
# MAGIC Enquanto, na primeira abordagem, consideramos que a casa de apostas acertou 0% o resultado final, nesta segunda consideramos que ela estimou em 16,92% a chance de vitória da equipe que ganhou a partida.
# MAGIC
# MAGIC Para ambas as abordagens, será necessário voltar a trabalhar com novas etapas de transformação da nossa tabela flat, acrescentando colunas e dados nela. 
# MAGIC
# MAGIC Desta forma, incluímos uma nova coluna chamada VencedorAposta. Ela indica através de um caractere (H = home, time da casa; D = draw, empate; A = away, visitante), qual o mais provável vencedor segundo a média das cotações das casas de apostas.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE silver.europa
# MAGIC ADD COLUMN VencedorAposta VARCHAR(1);
# MAGIC
# MAGIC UPDATE silver.europa
# MAGIC SET VencedorAposta = CASE
# MAGIC     WHEN MediaH < MediaD AND MediaH < MediaA THEN 'H'
# MAGIC     WHEN MediaD < MediaH AND MediaD < MediaA THEN 'D'
# MAGIC     ELSE 'A'
# MAGIC END;

# COMMAND ----------

# MAGIC %md
# MAGIC A partir daí podemos tentar responder à pergunta sobre o índice de acerto absoluto das casas de apostas. Neste caso, em 55,05% dos casos a equipe que tinha melhores cotações de fato venceu a partida.

# COMMAND ----------

# MAGIC %sql
# MAGIC --percentual de acertos das casas de aposta - abordagem absoluta
# MAGIC SELECT FORMAT_NUMBER((COUNT(*) * 100.0 / (SELECT COUNT(*) FROM bronze.europa)), 2) AS percentual
# MAGIC FROM silver.europa
# MAGIC WHERE FTR = VencedorAposta;

# COMMAND ----------

# MAGIC %md
# MAGIC Para tratarmos a segunda abordagem, ponderando o peso de cada cotação, vamos precisar novamente enriquecer a tabela flat, incluindo novas informações vindas de cálculos com base na própria tabela. Primeiro os percentuais absolutos para cada resultado. No exemplo da partida Barcelona x Girona, é quele em que a soma das probabilidades ficou em 105,12%, incluindo a margem das casas de apostas.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE silver.europa ADD COLUMN PercentAbsolH FLOAT;
# MAGIC ALTER TABLE silver.europa ADD COLUMN PercentAbsolD FLOAT;
# MAGIC ALTER TABLE silver.europa ADD COLUMN PercentAbsolA FLOAT;
# MAGIC
# MAGIC UPDATE silver.europa
# MAGIC SET PercentAbsolH = (1 / MediaH) * 100,
# MAGIC     PercentAbsolD = (1 / MediaD) * 100,
# MAGIC     PercentAbsolA = (1 / MediaA) * 100;

# COMMAND ----------

# MAGIC %md
# MAGIC Utilizando os dados destas 3 colunas de apoio, criaremos novas 3 colunas, desta vez com o valor correto para a probabilidade de cada resultado segundo a média das casas de apostas (desta vez, a soma dos resultados será de 100%).

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE silver.europa ADD COLUMN PercentH FLOAT;
# MAGIC ALTER TABLE silver.europa ADD COLUMN PercentD FLOAT;
# MAGIC ALTER TABLE silver.europa ADD COLUMN PercentA FLOAT;
# MAGIC
# MAGIC UPDATE silver.europa
# MAGIC SET PercentH = (PercentAbsolH * 100) / (PercentAbsolH + PercentAbsolD + PercentAbsolA),
# MAGIC     PercentD = (PercentAbsolD * 100) / (PercentAbsolH + PercentAbsolD + PercentAbsolA),
# MAGIC     PercentA = (PercentAbsolA * 100) / (PercentAbsolH + PercentAbsolD + PercentAbsolA);
# MAGIC
# MAGIC SELECT * FROM silver.europa;

# COMMAND ----------

# MAGIC %md
# MAGIC Para facilitar as operações, mais uma coluna será incluída, consolidando a cotação do resultado efetivamente ocorrido. Apesar de ser possível fazer esta operação sem a necessidade de criação desta coluna, achamos que isso traria mais clareza na visualização dos resultados. 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE silver.europa ADD COLUMN Percentual FLOAT;
# MAGIC
# MAGIC UPDATE silver.europa 
# MAGIC SET Percentual = 
# MAGIC     CASE 
# MAGIC         WHEN FTR = 'H' THEN PercentH
# MAGIC         WHEN FTR = 'D' THEN PercentD
# MAGIC         WHEN FTR = 'A' THEN PercentA
# MAGIC         ELSE 0
# MAGIC     END;

# COMMAND ----------

# MAGIC %md
# MAGIC Finalmente, podemos chegar ao índice de acerto na predição das casas de apostas, considerando o peso de suas cotações: 42,31%.

# COMMAND ----------

# MAGIC %sql
# MAGIC --percentual de acertos das casas de aposta - abordagem ponderada
# MAGIC SELECT FORMAT_NUMBER((SUM(Percentual) / COUNT(Percentual)), 2) AS percentual_final
# MAGIC FROM silver.europa;

# COMMAND ----------

# MAGIC %md
# MAGIC Se considerarmos que cada partida pode ter 3 resultados (empate ou vitória de cada um dos times), o que daria 33,33% de chances para cada um, podemos dizer que as casas de apostas tem um índice considerável de acertos, tanto abolutamente quanto ponderado pela cotação.

# COMMAND ----------

# MAGIC %md
# MAGIC #### - Alguma liga segue mais o padrão destas previsões? Alguma está mais propensa a “zebras”?
# MAGIC Novamente vamos usar tanto os resultados absolutos das partidas quanto os ponderados pelas cotações.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT europa.League, 
# MAGIC        FORMAT_NUMBER((COUNT(*) * 100.0 / total_por_div), 2) AS percentual_div--,
# MAGIC        --FORMAT_NUMBER((COUNT(*) * 100.0 / (SELECT COUNT(*) FROM silver.europa )), 2) AS percentual_total
# MAGIC FROM silver.europa
# MAGIC INNER JOIN (
# MAGIC     SELECT League AS subquery_Div, COUNT(*) AS total_por_div
# MAGIC     FROM silver.europa
# MAGIC     GROUP BY League
# MAGIC ) AS subquery ON europa.League = subquery.subquery_Div
# MAGIC WHERE FTR = VencedorAposta
# MAGIC GROUP BY europa.League, total_por_div
# MAGIC ORDER BY percentual_div DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC Para valores absolutos, as ligas espanhola, italiana e alemã ficaram mais próximas da média de 55,05% de acerto. Chama a atenção a liga inglesa, com índice de acerto bem acima da média, e a francesa que ficou quase 5% abaixo.

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT League, FORMAT_NUMBER(SUM(Percentual) / COUNT(Percentual), 2) AS soma_dividida
# MAGIC FROM silver.europa
# MAGIC GROUP BY League
# MAGIC ORDER BY soma_dividida DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC Para os valores ponderados, podemos ver que a tendência se mantém, com a liga inglesa acima e a francesa abaixo da média de 42,31%.
# MAGIC
# MAGIC Entretanto, temos um outro dado interessante: desta vez a Alemanha ficou acima da média, e a Espanha abaixo, ao contrário do que ocorreu na análise anterior. Ou seja, embora as casas de apostas tenham mais vezes acertado o vencedor final das partidas da liga espanhola, na liga alemã os acertos foram mais assertivos, com as casas cotando probabilidades maiores para os jogos que acertou.
# MAGIC
# MAGIC Por fim, cabe mencionar que, se estivéssemos trabalhando em um Data Warehouse, também poderíamos usar uma dimensão localização (ou, no caso, a liga) para segmentar os resultados.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####- Em algum período da temporada, as probabilidades de apostas se confirmam mais ou menos do que em outros períodos?
# MAGIC Nossa hipótese inicial era que, nos primeiros jogos da temporada, as casas de aposta teriam mais dificuldade em “prever” os resultados das partidas, pois haveria menos histórico para calcular estas chances. À medida que os meses passassem, o percentual de acerto aumentaria.
# MAGIC
# MAGIC Portanto, nossa ideia será criar uma consulta para segmentar por mês os índices de acertos, tanto absolutos quanto relativos. Em um Data Warehouse, também poderíamos usar a dimensão tempo para trazer estes resultados.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --percentual absoluto de acertos das casas de apostas, segmentado por mês
# MAGIC SELECT mes,
# MAGIC        FORMAT_NUMBER((COUNT(*) * 100.0 / total_por_mes), 2) AS percentual
# MAGIC FROM (
# MAGIC     SELECT MONTH(DateMatch) AS mes,
# MAGIC            COUNT(*) AS total_por_mes
# MAGIC     FROM silver.europa
# MAGIC     GROUP BY MONTH(DateMatch)
# MAGIC ) AS subquery
# MAGIC JOIN silver.europa 
# MAGIC     ON MONTH(silver.europa.DateMatch) = subquery.mes
# MAGIC WHERE FTR = VencedorAposta
# MAGIC GROUP BY mes, total_por_mes
# MAGIC ORDER BY percentual DESC;

# COMMAND ----------

# MAGIC %sql 
# MAGIC --percentual ponderado de acertos das casas de apostas, segmentado por mês
# MAGIC SELECT MONTH(DateMatch) AS mes, 
# MAGIC   FORMAT_NUMBER(SUM(Percentual) / COUNT(Percentual), 2) AS Percentual_mes
# MAGIC FROM silver.europa
# MAGIC GROUP BY mes 
# MAGIC ORDER BY Percentual_mes DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC Ao agruparmos os resultados por mês, não vemos uma lógica na distribuição. Tanto na abordagem absoluta quanto na relativa pelas cotações, temos meses do início e do final da temporada tanto acima quanto abaixo da média, sem seguir um padrão. Logo, a resposta que os dados trouxeram para essa pergunta não corroborou a hipótese levantada.

# COMMAND ----------

# MAGIC %md
# MAGIC ####- Há alguma distorção entre as probabilidades que cada empresa trabalha antes das partidas, ou elas seguem o mesmo padrão?
# MAGIC Apesar do catálogo de dados inicial indicar que o dataset trazia dados de 12 casas de apostas, verificamos na seção “Qualidade de dados” que na verdade apenas as cotações de 6 empresas constavam nos arquivos. São elas: 
# MAGIC
# MAGIC   Bet365 
# MAGIC   Bet&Win 
# MAGIC   Interwetten 
# MAGIC   Pinnacle 
# MAGIC   VC Bet 
# MAGIC   William Hill 
# MAGIC
# MAGIC Para verificar se as empresas trabalham com cotações parecidas, fizemos uma análise do desvio padrão para a cotação de vitória do time da casa, empate e vitória do visitante para cada uma das empresas, calculando o seu desvio padrão da média das cotações.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --desvio padrão das cotações de cada casa de aposta
# MAGIC SELECT "Bet365" AS Casa_aposta,
# MAGIC        ROUND(STDDEV(B365H - MediaH), 3) AS DesvioPadraoHome,
# MAGIC        ROUND(STDDEV(B365D - MediaD), 3) AS DesvioPadraoDraw,
# MAGIC        ROUND(STDDEV(B365A - MediaA), 3) AS DesvioPadraoAway
# MAGIC FROM silver.europa
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT "Bet&Win" AS Casa_aposta,
# MAGIC        ROUND(STDDEV(BWH - MediaH), 3) AS DesvioPadraoHome,
# MAGIC        ROUND(STDDEV(BWD - MediaD), 3) AS DesvioPadraoDraw,
# MAGIC        ROUND(STDDEV(BWA - MediaA), 3) AS DesvioPadraoAway
# MAGIC FROM silver.europa
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT "Interwetten" AS Casa_aposta,
# MAGIC        ROUND(STDDEV(IWH - MediaH), 3) AS DesvioPadraoHome,
# MAGIC        ROUND(STDDEV(IWD - MediaD), 3) AS DesvioPadraoDraw,
# MAGIC        ROUND(STDDEV(IWA - MediaA), 3) AS DesvioPadraoAway
# MAGIC FROM silver.europa
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT "Pinnacle" AS Casa_aposta,
# MAGIC        ROUND(STDDEV(PSH - MediaH), 3) AS DesvioPadraoHome,
# MAGIC        ROUND(STDDEV(PSD - MediaD), 3) AS DesvioPadraoDraw,
# MAGIC        ROUND(STDDEV(PSA - MediaA), 3) AS DesvioPadraoAway
# MAGIC FROM silver.europa
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT "VC Bet" AS Casa_aposta,
# MAGIC        ROUND(STDDEV(VCH - MediaH), 3) AS DesvioPadraoHome,
# MAGIC        ROUND(STDDEV(VCD - MediaD), 3) AS DesvioPadraoDraw,
# MAGIC        ROUND(STDDEV(VCA - MediaA), 3) AS DesvioPadraoAway
# MAGIC FROM silver.europa
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT "William Hill" AS Casa_aposta,
# MAGIC        ROUND(STDDEV(WHH - MediaH), 3) AS DesvioPadraoHome,
# MAGIC        ROUND(STDDEV(WHD - MediaD), 3) AS DesvioPadraoDraw,
# MAGIC        ROUND(STDDEV(WHA - MediaA), 3) AS DesvioPadraoAway
# MAGIC FROM silver.europa;

# COMMAND ----------

# MAGIC %md
# MAGIC Podemos observar que o desvio padrão é menor e mais uniforme nas cotações de vitória do time da casa.
# MAGIC Nossa hipótese é que isso ocorre porque as equipes que jogam em casa têm maior probabilidade de vencer a partida do que as equipes visitantes, conforme podemos verificar numa consulta a todas as partidas da temporada. Logo, probabilidades mais altes significam cotações mais baixas e, consequentemente, os desvios também são menores.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --Resultado das partidas do dataset
# MAGIC SELECT 
# MAGIC        FORMAT_NUMBER((SUM(CASE WHEN HTR = 'H' THEN 1 ELSE 0 END)/COUNT(*) * 100.0 ), 2) AS vitoria_casa,
# MAGIC        FORMAT_NUMBER((SUM(CASE WHEN HTR = 'D' THEN 1 ELSE 0 END)/COUNT(*) * 100.0 ), 2) AS empate,
# MAGIC        FORMAT_NUMBER((SUM(CASE WHEN HTR = 'A' THEN 1 ELSE 0 END)/COUNT(*) * 100.0 ), 2) AS vitoria_visitante
# MAGIC FROM silver.europa;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Já para vitória do visitante, o desvio padrão entre as cotações de cada empresa foi maior. Por ser um resultado mais improvável, supomos que cada empresa deve ter uma política diferente para definir estas “odds” e, por consequência, atrair apostadores em busca de ganhos maiores.

# COMMAND ----------

# MAGIC %md
# MAGIC ##Autoavaliação
# MAGIC Inicialmente, a ideia deste trabalho seria trabalhar alguma forma de detectar indícios de fraude em apostas esportivas. Mas isso requereria uma base com dados de volumes de apostas e de quantias financeiras envolvidas. Estas informações, entretanto, não são abertas pelas empresas, e esta possibilidade foi descartada no início.
# MAGIC
# MAGIC Entretanto, na procura por estes dados, encontrei este dataset, que embora pequeno, trouxe uma massa de dados suficiente para elaborar algumas perguntas e, a partir disso, levantar hipóteses sobre como funciona o mercado de apostas por parte das empresas que comercializam este serviço, comprovando-as ou não. 
# MAGIC
# MAGIC Talvez trabalhar com perguntas que tivessem respostas mais assertivas ao invés de hipóteses permitiria concentrar mais em outros aspectos do trabalho, mas este foi um desafio que gostei de ter aceito.
# MAGIC
# MAGIC Para tentar extrair mais informações deste mesmo dataset, vejo a possibilidade de tentar correlacionar estatísticas de partidas (chutes a gol, escanteios, faltas etc.) com as cotações. Será que estes dados entram na equação onde elas calculam as probabilidades de vitória de cada equipe? Creio que isso iria demandar um conhecimento em estatística que iria além do básico, e por isso não foi possível seguir com perguntas relacionadas a estes dados para a entrega deste MVP. Mas isso pode ser explorado futuramente.
# MAGIC
# MAGIC Outro motivo da escolha deste dataset foi o potencial dele ser utilizado nos outros MVPs do curso, podendo ser base para trabalhos de machine learning, por exemplo.
# MAGIC
# MAGIC Apesar de tentar seguir o desenvolvimento do trabalho de acordo com a sequência proposta pelo enunciado do problema, percebi que em algumas vezes era necessário retornar a alguma etapa anterior. Por exemplo, quando já estava no trabalho da responder a uma pergunta, precisei retornar ao trabalho de transformação dos dados, para apurar alguma informação necessária. Achei mais didático manter uma ordem cronológica, mesmo que para isso estes pontos ficassem fora da seção do trabalho mais adequada. Espero que esta escolha ajude no entendimento de quem ler este trabalho.  
# MAGIC
# MAGIC Particularmente, a minha maior dificuldade no trabalho foi definir a plataforma, entender o básico de seu funcionamento e da configuração do ambiente. Diria que no mínimo 60% do tempo deste trabalho foi dedicado a estas atividades. Mas foi fundamental esse entendimento sobre Databricks para conseguir enfim realizar as atividades de ETL e explorar as consultas.
# MAGIC
