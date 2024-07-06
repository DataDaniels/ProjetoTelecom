# Use Case: compreendendo os desafios

Uma empresa de telecomunicações oferece serviços de internet de alta velocidade para uma ampla base de clientes residenciais e empresariais. Sua infraestrutura de rede inclui vários servidores distribuídos em diferentes locais para garantir a qualidade do serviço.

O engenheiro de dados é responsável pelo monitoramento da rede e dos servidores da empresa. Ele enfrenta desafios diários para assegurar que a infraestrutura opere de maneira eficiente, proporcionando a melhor experiência de conexão possível aos clientes. Com o aumento da demanda por serviços de internet, ele percebeu que alguns servidores estão sobrecarregados e precisam ser atualizados para atender à demanda crescente.

Para identificar os servidores que necessitam de upgrade, o engenheiro de dados monitora em tempo real o número de dispositivos conectados a cada servidor e a taxa de uso das conexões. Além disso, ele identifica os clientes que consomem uma quantidade significativa de largura de banda para garantir que recebam um serviço de qualidade.

Ao analisar os dados, o engenheiro de dados descobriu que a empresa está enfrentando um aumento no número de dispositivos conectados, causando lentidão na rede e impactando a experiência do cliente. Ele também identificou alguns clientes que estão utilizando uma quantidade desproporcional de largura de banda, afetando a conexão de outros clientes.

Diante desses desafios, o engenheiro de dados propôs uma atualização dos servidores para aumentar a capacidade de conexão e melhorar a qualidade do serviço. Ele também sugeriu a implementação de medidas para identificar e limitar o uso excessivo de largura de banda por parte de alguns clientes, garantindo uma experiência equitativa para todos os usuários da rede.


# Principais perguntas de negócio a serem respondidas

Quantos dispositivos se conectaram e desconectaram em tempo real?

Quantos dispositivos se conectaram nos últimos 10 minutos?

Qual a Largura de banda em uso em tempo real?

Qual a taxa de cliente de alta conexão conectados?

Qual a largura de banda entre dispositivos?


# Arquitetura das pipe-lines: Engenharia de dados para times de dados e negócios

<p align="center">
  <img src="https://drive.google.com/uc?export=view&id=17lpbbUrs5HXUGJ_p6bmot0bpKk7vtgE3" alt="1" style="width: 1200px;"/>
</p>

<p align="center">
  <img src="https://drive.google.com/file/d/1-14W47bnAa5bX2xoaFyArtwAOLPjEw5r" alt="2" style="width: 1200px;"/>
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


<p align="center">
  <img src="https://drive.google.com/file/d/1-14W47bnAa5bX2xoaFyArtwAOLPjEw5r/view?usp=sharing" alt="2" style="width: 1200px;"/>
</p>

```sql
%sql
CREATE DATABASE IF NOT EXISTS spark_catalog.bronze
LOCATION 'dbfs/FileStore/bronze/';
```

