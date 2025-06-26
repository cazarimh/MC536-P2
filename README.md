# MC536 - Projeto 2 – Banco de Dados: Análise das emissões de gases do efeito estufa no Brasil a fim de promover uma agricultura sustentável

## Sumário
* [Sobre o projeto](#sobre-o-projeto)
* [Justificativa](#justificativa)
* [Modelagem do Banco de Dados](#modelagem-do-banco-de-dados)
* [Tecnologias utilizadas](#tecnologias-utilizadas)
* [Datasets](#datasets)
* [Consultas Geradas](#consultas-geradas)
* [Resultados](#resultados)

## Sobre o projeto
Autores:
* [Luiz Felipe Lenharo](https://github.com/luizlenharo) (237896)
* [Henrique Cazarim Meirelles Alves](https://github.com/cazarimh) (244763)
* [Gustavo Marcelino Rodrigues](https://github.com/gustavomrodrigues) (238183)

<br>Esse projeto foi desenvolvido durante a disciplina MC536 - Banco de Dados: Teoria e Prática. O objetivo deste projeto consiste em idealizar um banco de dados não relacional utilizando datasets distintos e realizar consultas neste banco. A realização do projeto se baseou nas ODS 2 (Fome zero e agricultura sustentável) e ODS 13 (Ação contra a mudança global do clima), escolhemos datasets que mostram emissões de GEE no Brasil e preservação de áreas das cidades do Brasil.
<br>A partir do banco de dados resultante do projeto é possível acessar informações sobre as emissões, como a quantidade emitida (de 1970 a 2023), a localização (estado e bioma) em que ocorreu e caracterização (setor, categoria, produto, etc.), além de dados sobre a divisão do tipo de área de cada município brasileiro (com área total, preservada, protegida e rural). As consultas geradas são a respeito dos GEE no Brasil provenientes principalmente de atividades agrícolas, como descrito nas seções seguintes.

## Justificativa

### 1. Forma de armazenamento de arquivos
<br>O Cenário B necessita de um formato de dados maleável, que possa lidar com informações que mudam constantemente e que não seguem um padrão rígido. Em nosso projeto, esse fator é importante para dados como as emissões por cidade, estado ou região (SEEG), ou informações sobre áreas protegidas (Embrapa). Esses dados podem ter informações extras ou faltantes, dependendo do lugar ou do tipo de área. Com um formato maleável, podemos adicionar novas informações sem a necessidade de alterar toda a estrutura do sistema.
<br>O MongoDB, que é um tipo de banco de dados que organiza as informações em documentos (usando um formato chamado BSON, baseado em JSON), é adequado para  a funcionalidade descrita. Ele permite guardar documentos com diferentes formatos dentro de um mesmo grupo, não exigindo um modelo fixo inicial. Para os dados da Embrapa e do SEEG, isso significa poder guardar todas as informações importantes sobre uma emissão ou uma área preservada em um único lugar, mesmo que essas informações mudem de um registro para outro.
<br>A inserção de informações novas pode ser feita sem a necessidade de alterar a estrutura do sistema ou fazer grandes mudanças no banco de dados. Essa forma de trabalhar, focada em "pedaços completos" de informação, auxilia na criação de APIs e no uso de dados de forma rápida.

### 2. Linguagem e processamento de consultas

<br>O MongoDB utiliza uma linguagem chamada MQL (MongoDB Query Language), que é similar com o JSON e é focada na procura e uso dos documentos. Quando os dados estão organizados em documentos completos, como os do SEEG ou da Embrapa, encontrar as informações é fácil e rápido, sem precisar fazer operações complicadas como juntar tabelas, o que ajuda o sistema a funcionar mais rápido.

### 3. Como os Dados São Processados e Controlados

<br>O MongoDB suporta transações ACID, o que garante que as operações sejam realizadas de maneira consistente. Para operações simples de leitura e escrita em documentos únicos, a atomicidade já é suficiente para o que precisamos no projeto, como nas APIs que usam informações completas.
<br>Quando não precisamos de uma consistência imediata e muito forte, o MongoDB permite usar formas de consistência mais flexíveis, como a consistência eventual, o que melhora o desempenho e a disponibilidade, fatores relevantes para monitorar dados em grande escala e em sistemas que precisam suportar uma alta demanda.

### 4. Processamento e controle de transações

<br>O MongoDB utiliza duas estratégias para resistir a eventuais falhas: replicar sets e sharding.
<br>Os Conjuntos de Réplicas são uma garantia de que o serviço não irá parar e que sempre haverá uma cópia de segurança. Cada conjunto é formado por um nó principal e outros nós, que são cópias exatas dos dados. Caso o nó principal falhe, um dos nós secundários assume o controle automaticamente, mantendo tudo funcionando.
<br>O Sharding, ou divisão automática de dados, é o que permite aumentar a capacidade do sistema de forma horizontal. A partir dele, é possível espalhar grandes quantidades de dados (como os do GEE ou de áreas que crescem rapidamente) por vários servidores (os shards), e o sistema cresce conforme a necessidade. O balanceamento da carga é automático, e recursos como o journaling protegem os dados se algo der errado.

### 5. Mecanismos de recuperação e segurança.

<br>Conjuntos de ferramentas do MongoDB associadas à recuperação e segurança:
<br>Autenticação: Aceita vários tipos de autenticação, como SCRAM e certificados X.509, e se integra com sistemas de autenticação de empresas via LDAP e Kerberos. Isso garante que só pessoas autorizadas acessem dados importantes.
<br>Controle de acesso por papéis (RBAC): Permite a criação de funções com permissões específicas, seguindo a ideia de fornecer a cada um apenas o acesso necessário. Assim, é possível controlar quem pode acessar os bancos de dados, as coleções e as operações.
<br>Criptografia: Criptografa os dados em trânsito (TLS/SSL), protegendo a comunicação entre as APIs e os servidores.
<br>Segurança de rede: Existem ferramentas para configurar regras de firewall, dividir a rede em partes e limitar os acessos por IP. Isso auxilia a isolar e proteger o banco de dados contra acessos não autorizados.


## Modelagem do Banco de Dados
### Imagem 1: Modelo Conceitual (Diagrama MER)
<p align="center">
  <img src="./models/modeloConceitual.png" alt="Modelo Conceitual" width="700"/>
</p>

### Imagem 2: Modelo Lógico
<p align="center">
  <img src="./models/modeloLogico.png" alt="Modelo Lógico" width="700"/>
</p>

<br>O [Modelo Físico](./models/modeloFisico.js) foi feito em JavaScript para integração com o MongoDB, o script de criação e definição das coleções pode ser encontrado [neste link](./models/modeloFisico.js).

## Tecnologias utilizadas
* **Banco de dados:** `MongoDB`  
* **Linguagem de Programação:** `Python, JavaScript`  
* **Bibliotecas:** `pandas, pymongo`  

## Datasets
### SEEG (Sistema de Estimativas de Emissões e Remoções de Gases de Efeito Estufa)
* Os dados estão diretamente atrelados aos setores de Processos Industriais e Resíduos, Mudança de uso da Terra, Energia e Agropecuária.
* A plataforma do [`SEEG`](https://seeg.eco.br/dados/) disponibiliza dados detalhados sobre emissão por município, estado, região e setor.
* O SEEG é fundamental para o monitoramento das mudanças climáticas e construção de medidas políticas voltadas para a redução da emissão dos gases de efeito estufa (GEE).

### EMBRAPA (Empresa Brasileira de Pesquisa Agropecuária)
* Os dados estão diretamente relacionados com as áreas preservadas do Brasil, sendo elas:
    * Área total do município
    * Número de propriedades rurais
    * Áreas protegidas (conservação, índigena e militar)
    * Áreas preservadas (destinadas a preservação vegetal em propriedade rural)

* A [`EMBRAPA`](https://geoinfo.dados.embrapa.br/metadados/srv/por/catalog.search#/metadata/61e66efd-7757-4d78-84b9-c3047a8bbc70) realiza e desenvolve pesquisas e tecnologias direcionadas à gestão sustentável de recursos naturais e o desenvolvimento sustentável.
* Alguns dos dados referentes às áreas neste dataset estão incongruentes. Notamos que alguns estão multiplicados por potência de 10 porém sem um padrão específico e não encontramos documentação que especificasse ou explicasse o motivo. Portanto, consultas geradas com dados referentes às áreas podem não refletir a realidade.

### Pré-Processamento 
#### SEEG
1. Deixamos apenas a página da planilha de dados.
5. Separamos a planilha de dados em 4 chunks de tamanho semelhante.
6. Salvamos cada uma das partes em [`gasesEE-medicoes_Ci.csv`](./dataset), sendo i o número de cada chunk

#### EMBRAPA
1. Excluimos as colunas A, B, C, D, H, W, X e Y pois que possuíam informações desnecessárias para nossa utilização.
2. Salvamos a planilha resultado em [`cidadesPreserv.csv`](./dataset/cidadesPreserv.csv)

#### SCRIPT
1. Utilizamos as planilhas resultantes para popular o banco

## Consultas Geradas
Para realizar a análise dos dados foram feitas cinco consultas não triviais em MQL, listadas abaixo:
1. Porcentagem da emissão da agropecuária sobre emissão total em um estado em determinado ano.

2. Evolução da (emissão agropecuária/área rural) e (emissão indústria/área urbana) ao longo dos anos.

3. Aumento da emissão de gases em um estado e nos demais no período de 1970 - 2023 e comparação entre aumento deste e média dos demais. <br> Obs: Aumento relativo mostra a intensidade do crescimento de um estado em relação ao resto do Brasil.

4. Porcentagem de emissao dos top 5 produtos mais emissores na agropecuária em uma determinada região e ano.

5. Top 10 anos com maior balanço *qtd_em* + *qtd_rem* no século 21.

O código delas se encontra [aqui](./querys/). As consultas também foram implementadas em Python e podem ser encontradas [aqui](./querys/querys.py).

## Resultados
Executando as consultas listadas acima, obtivemos os resultados presentes no diretório [/results](./results/), contendo os arquivos `.json` resultante de cada uma das consultas.
