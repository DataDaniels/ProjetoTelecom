# Use Case: compreendendo os desafios

Uma empresa de telecomunicações oferece serviços de internet de alta velocidade para uma ampla base de clientes residenciais e empresariais. Sua infraestrutura de rede inclui vários servidores distribuídos em diferentes locais para garantir a qualidade do serviço.

O engenheiro de dados é responsável pelo monitoramento da rede e dos servidores da empresa. Ele enfrenta desafios diários para assegurar que a infraestrutura opere de maneira eficiente, proporcionando a melhor experiência de conexão possível aos clientes. Com o aumento da demanda por serviços de internet, ele percebeu que alguns servidores estão sobrecarregados e precisam ser atualizados para atender à demanda crescente.

Para identificar os servidores que necessitam de upgrade, o engenheiro de dados monitora em tempo real o número de dispositivos conectados a cada servidor e a taxa de uso das conexões. Além disso, ele identifica os clientes que consomem uma quantidade significativa de largura de banda para garantir que recebam um serviço de qualidade.

Ao analisar os dados, o engenheiro de dados descobriu que a empresa está enfrentando um aumento no número de dispositivos conectados, causando lentidão na rede e impactando a experiência do cliente. Ele também identificou alguns clientes que estão utilizando uma quantidade desproporcional de largura de banda, afetando a conexão de outros clientes.

Diante desses desafios, o engenheiro de dados propôs uma atualização dos servidores para aumentar a capacidade de conexão e melhorar a qualidade do serviço. Ele também sugeriu a implementação de medidas para identificar e limitar o uso excessivo de largura de banda por parte de alguns clientes, garantindo uma experiência equitativa para todos os usuários da rede.


# Principais perguntas iniciais de negócio a serem respondidas

Quantos dispositivos se conectaram e desconectaram em tempo real?

Quantos dispositivos se conectaram nos últimos 10 minutos?

Qual a Largura de banda em uso em tempo real?

Qual a taxa de cliente de alta conexão conectados?

Qual a largura de banda entre dispositivos?


# Arquitetura das pipe-lines: Engenharia de dados para times de dados e negócios

<p align="center">
  <img src="https://drive.google.com/uc?export=view&id=17lpbbUrs5HXUGJ_p6bmot0bpKk7vtgE3" alt="1" style="width: 1200px;"/>
</p>

- Data Source: Os dados são originados em várias fontes.

- INPUT é feito através das Aplicações de Devices: Os dados são capturados pelas aplicações nos dispositivos e enviados para a landing-zone.

- PROCESS Arquitetura Multi-Hope (Medallion)

  - Landing-zone: Os dados recém-chegados são armazenados inicialmente na landing-zone.

  - Bronze Zone (camada bronze): Nesta camada, os dados brutos da landing-zone são armazenados, mantendo sua integridade original.

  - Silver Zone (camada prata): Os dados da bronze zone são processados e limpos para garantir qualidade e consistência, preparando-os para análises mais avançadas.

  - Gold Zone (camada outro): Na última camada, os dados são refinados, agregados e otimizados para facilitar análises complexas e relatórios detalhados.

- OUTPUT pelo Databricks: neste projeto os dados são disponibilizados pelo Databrics porém isso pode ser feito por outras ferramentas como na imagem e os dados podem ser trabalhados em diversas ferramentas de visualização posteriormente.

# Criando a estrutura do projeto

## Criando o Cluster

Para iniciar qualquer projeto em uma plataforma de cloud como a Databricks, é necessário criar um cluster que nada mais é do que nossa "máquina", o computador que será responsável por processar todo o projeto.

![Criação Cluster](https://github.com/DataDaniels/imagensprojetotelecom/blob/main/cluster.png)

## Criação das camadas (Arquitetura MultiHop - Medallion)

É necessário antes de tudo, criar as camadas ou seja, os bancos de dados que irão corresponder às camadas bronze, prata e ouro.

![Arquitetura Multi-Hop](https://github.com/DataDaniels/imagensprojetotelecom/blob/main/bd_camadas.png?raw=true)

```sql
%sql
CREATE DATABASE IF NOT EXISTS spark_catalog.bronze
LOCATION 'dbfs/FileStore/bronze/';
```
```sql
%sql
CREATE DATABASE IF NOT EXISTS spark_catalog.prata
LOCATION 'dbfs/FileStore/prata/';
```
```sql
%sql
CREATE DATABASE IF NOT EXISTS spark_catalog.gold
LOCATION 'dbfs/FileStore/ouro/';
```

## Criando aplicação: estrutura para gerar e organizar dados

Nesta etapa veremos a criação de funções que serão responsáveis por gerar os arquivos que serão analisados posteriormente e organizá-los. É importante dizer que os dados são ficticios, foram gerados por uma Lib chamada Faker que gera dados falsos, simulando informações como nomes, endereços, números de telefone, e-mails, entre outros, de forma aleatória e realista.

```python
#Instalando a Lib de dados fake.
!pip install Faker
## https://faker.readthedocs.io/en/master/
```
```python
#Importando as libs necessárias
from faker import Faker
import random
from pyspark.dbutils import DBUtils
from faker.providers import internet
from datetime import datetime
import pandas as pd
import os
```
A função funcao_renomear é definida para mover arquivos Parquet de um diretório para outro. Ela itera sobre os arquivos em um diretório especificado (local), verifica se o nome do arquivo termina com .parquet, copia-o para um novo local (localnovo), e então remove o diretório original após a cópia. Finalmente, a função imprime onde o arquivo foi gerado no novo local.

```python
def funcao_renomear(local, localnovo):

    from datetime import datetime

    for file_name in dbutils.fs.ls(local):
        file_name = file_name[0] #pega o nome somente
        if file_name[-8:] == '.parquet': #se o final termina com .parquet
            dbutils.fs.cp(file_name, localnovo) #move o parquet com outro nome para o lugar certo
            dbutils.fs.rm(local, True) #apaga a pasta gerada

    return print(f'Arquivo gerado em: {localnovo}')
```

Neste bloco abaixo são gerados dados fictícios usando a biblioteca Faker. A função gerar_dados cria uma lista de registros fictícios, cada um contendo nome, endereço, IP, hora de conexão, tipo de dispositivo, velocidade de conexão e status de conexão. Esses registros são convertidos em um DataFrame usando Pandas e depois em um DataFrame Spark. O DataFrame Spark é então salvo como arquivo Parquet na zona de aterrissagem (landing_zone). Após salvar, a função chama funcao_renomear (do Bloco 1) para renomear o arquivo Parquet recém-criado na landing-zone. A função gerar_dados retorna uma mensagem indicando o sucesso da operação e detalhes sobre o número de conexões, tipo de dispositivo, status e local do arquivo.

```python
#Gerando dados

fake = Faker()
fake.add_provider(internet)

def gerar_dados(quantidade_conexao,dispositivo_de_acesso,status):
# Criando Lista Vazia 
    fake_records = []
    velocidade_conexao = [1, 5, 10, 15, 25, 35, 50, 100, 200, 400, 500, 1000]

# Gerando lista de dados fakes
    for _ in range(quantidade_conexao):
        novo_registro = {
            'Nome': fake.name(),
            'Endereco': fake.address(),
            'Ip':fake.ipv4_private(),
            'Hora_Conexao' : datetime.now(),
            'Dispositivo_de_Acesso' : dispositivo_de_acesso,
            'Velocidade_de_Conexao' : random.choice(velocidade_conexao),
            'Status_Conexao' : status
            }
        fake_records.append(novo_registro)

    # Criando Dataframe apartir dos dados gerados
    df = pd.DataFrame.from_records(fake_records)
    #convertendo pandas em dataframe spark
    spark_df = spark.createDataFrame(df)
    #salvando na landing_zone
    try:
        #Local de Geração do Path do arquivo gerado
        Local = f"dbfs:/FileStore/landing_zone/{dispositivo_de_acesso}/{status}/{status}_{datetime.now()}"
        #Gravar arquivo
        spark_df.coalesce(1).write.format("parquet").mode("overwrite").save(Local)
        #mover arquivo
        local_novo = f"dbfs:/FileStore/landing_zone/{dispositivo_de_acesso}/{status}/{dispositivo_de_acesso}/{dispositivo_de_acesso}_{datetime.now()}.parquet"

        funcao_renomear(Local,local_novo)

        return print (f'Gerado com Sucesso = {quantidade_conexao} conexões de {dispositivo_de_acesso} | Status: {status} | Local do Arquivo: {Local}')
    
    except:
        return print('Falha no processo! Função gerar_dados().')
```
A função ingestion_gerar_dado é definida para facilitar a chamada da função gerar_dados com base em um argumento específico. Ele usa um dicionário para mapear argumentos para diferentes chamadas de gerar_dados, cada uma configurada com parâmetros específicos para quantidade de conexões, tipo de dispositivo e status de conexão. Isso permite uma execução condicional e dinâmica com base no argumento fornecido à função.

```python
def ingestion_gerar_dado(argument):
    switch_case = {
        'Computador_Conectado': lambda: gerar_dados(40, 'Computador', 'Conectado'),
        'Celular_Conectado': lambda: gerar_dados(80, 'Celular', 'Conectado'),
        'Computador_Desconectado': lambda: gerar_dados(70, 'Computador', 'Desconectado'),
        'Celular_Desconectado': lambda: gerar_dados(60, 'Celular', 'Desconectado')
    }

    # Executa a função correspondente ao argumento fornecido
    switch_case.get(argument, lambda: print('Caso não encontrado!'))()

## Exemplo de uso
#ingestion_gerar_dado('Computador_Conectado')
```
## Gerando arquivos na landing zone

Através de nossa aplicação são gerados os arquivos e será necessário realizar este processo de acordo com cada tipo de dispositivo e seu status. E estes são, Celular Conectado e Desconectado, Computador Conectado e Desconectado. 
Abaixo segue o código necessário para rodar nossa aplicação e realizar ingestão em nossa land-zone bem como a query SQL necessária para realizar a consulta e confirmar a ingestão dos arquivos.
Obs.: para gerar dados com os demais tipos de dispositivo e seus status, será necessário substituir no código conforme o desejado. 
```python
%run "./aplication"
```
```python
import time as t

while(True):
    t.sleep(10)
    ingestion_gerar_dado('Celular_Conectado')
```
Query SQL que realiza a consulta para verificar a quantidade de registros de celulares conectados em nossa landing zone.
```sql
%sql
SELECT count(*) FROM parquet.`dbfs:/FileStore/landing_zone/Celular/Conectado/Celular/`
```
![Query camada bronze](https://github.com/DataDaniels/imagensprojetotelecom/blob/main/query%20landing%20zone.png)

## Ingestão e strutered streaming
Para a ingestão e 

```python
import time as t

while(True):
    t.sleep(10)
    ingestion_gerar_dado('Celular_Conectado')
```#Importando apenas libs necessárias para essa execução 
from datetime import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from collections import OrderedDict ,Counter
from functools import reduce 
from pyspark.sql.streaming import * 
from delta.tables import * 
from itertools import chain
```
```python
#Listando nosso DBFS, nosso "Storage"
dbutils.fs.ls("dbfs:/FileStore/landing_zone/Celular/Conectado/Celular/")
```
```python
#Input
origem_location = 'dbfs:/FileStore/landing_zone/Celular/Conectado/Celular/' #origem do dado
#Output
destino_location = 'dbfs:/FileStore/bronze/Celular/Conectado/Celular/' #destino do dado
tabela_destino = 'spark_catalog.bronze.celular_conectado' # destino tabela acesso
checkpoint = 'dbfs:/FileStore/bronze/Celular/Conectado/Celular_chk/' # Definir o diretório de checkpoint
schema = 'dbfs:/FileStore/bronze/Celular/Conectado/Celular_schema/' # Definir o Local do meu schema
source = 'Celular Conectado'
```
```python
#Lendo os microbatch novos para o streaming
streamingDF = (spark.readStream.format('cloudFiles')\
    .option('cloudFiles.Format', 'parquet')\
    .option('cloudFiles.inferColumnTypes', 'true')\
    .option('cloudFiles.schemaLocation', schema)\
    .option('cloudFiles.schemaEvolutionMode', 'addNewColumns')\
    .load(origem_location)\
     #Adicionando o caminho do arquivo fonte da ingestão   
     .withColumn('rastreamento_source',input_file_name())\
     #Adicionando a fonte da informação 
     .withColumn('source',lit(source)) \
     #Adicionando a data/hora do arquivo da fonte 
     .withColumn("data_arquivo_ingestao", col("_metadata.file_modification_time")) \
     #Adicionando um campo extra, caso seja necessário futuramente criar flag    
     .withColumn('status',lit(True)))
```
```python
# Escrever o stream de dados processados em outro diretório
query = (streamingDF 
    .writeStream
    # .queryName("spark_catalog.bronze.celular_conectado") #Nome da Tabela de saida
    .format("delta")  # Formato de dados de saída
    .outputMode("append")   # Modo de saída (append, complete, update)
    .option("checkpointLocation", checkpoint)   # Localização do checkpoint
    .option("path", destino_location)   # Diretório de saída
    .trigger(availableNow=True) #Processa tudo que tem para processar e termina a execução
    .table(tabela_destino)
)
```
Query SQL para consultar os dados presentes na camada bronze após a ingestão
```sql
%sql 
select count (*) from bronze.celular_conectado
```
![Query camada bronze](https://github.com/DataDaniels/imagensprojetotelecom/blob/5aff8dc912ae278d0e6253f3b3dfa4ed1d6ecf2e/query%20camada%20bronze%20count.png)

## Habilitando o Change Data Feed (CDF)

O Change Data Feed (CDF) é uma funcionalidade que permite capturar e registrar mudanças em dados de um banco de dados em tempo real ou quase real. Ele monitora operações como inserções, atualizações e exclusões, armazenando essas alterações em um log de mudanças. O CDF é útil para replicação de dados, sincronização entre sistemas e auditoria, proporcionando atualizações eficientes sem a necessidade de varrer todas as linhas de uma tabela para identificar mudanças. 

```sql
%sql
-- Habilitando o CDF(Change Data Feed para todas as tabelas)
set spark.databricks.delta.properties.defaults.enableChangeDataFeed = true;
```
![Habilitando CDF](https://github.com/DataDaniels/imagensprojetotelecom/blob/51a2e3fe7c9df3cc24b1c7f5338f9e6e55e7ced7/Habilitando%20CDF%20.png)

