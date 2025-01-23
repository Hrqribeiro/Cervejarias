# Cervejarias
# Pipeline de Dados para Cervejarias

Este projeto consome dados da Open Brewery DB API, transforma e armazena os dados em um data lake seguindo a arquitetura de medalhão (bronze, silver, gold).

## Estrutura do Projeto

- **`pipeline.py`**: Código do DAG do Airflow que orquestra o pipeline.
- **`data/`**: Pasta para armazenar os dados nas camadas bronze, silver e gold.
- **`README.md`**: Documentação do projeto.
- **`requirements.txt`**: Lista de dependências do projeto.

## Requisitos

- Python 3.9
- Bibliotecas: Verifique o arquivo `requirements.txt`.

## Como Executar

1. **Instale as Dependências**:
   ```bash
   pip install -r requirements.txt
Configure o Airflow:

Inicialize o banco de dados do Airflow:

bash
Copy
airflow db init
Crie um usuário:

bash
Copy
airflow users create --username admin --firstname SeuNome --lastname SeuSobrenome --role Admin --email seuemail@example.com
Configure as Conexões:

#No Airflow, crie uma conexão com o Databricks:

Conn Id: databricks_brewery_conn

Conn Type: Databricks

Host: URL do seu workspace do Databricks (ex: https://<seu-workspace>.cloud.databricks.com).

Token: Token de API do Databricks.

(Opcional) Configure a conexão com o Slack para notificações.

Execute o Pipeline:

#Inicie o servidor web do Airflow:

bash
Copy
airflow webserver --port 8080
Inicie o scheduler:

bash
Copy
airflow scheduler
Acesse a interface do Airflow (http://localhost:8080) e execute o DAG brewery_pipeline.

#Escolhas de Design
Arquitetura de Medalhão:
Os dados são armazenados em três camadas (bronze, silver, gold) para garantir qualidade e escalabilidade.

#Orquestração com Airflow:
O Airflow foi escolhido por sua flexibilidade e suporte a agendamento, retentativas e monitoramento.

#Notificações:
Foram implementadas notificações por e-mail e Slack para alertar sobre falhas ou sucesso na execução do pipeline.

#Compensações
Simplicidade vs. Complexidade: Optamos por um design simples, mas escalável, que pode ser facilmente adaptado para cenários mais complexos.

#Monitoramento:
As notificações básicas atendem ao requisito, mas em um ambiente de produção, ferramentas como Prometheus e Grafana seriam recomendadas.

#Tratamento de Erros
Retentativas: O pipeline está configurado para tentar novamente em caso de falha (3 tentativas com intervalo de 5 minutos).

#Notificações:
Em caso de falha, o Airflow envia um e-mail para o administrador.

#Logs:
Todos os logs são armazenados no Airflow para facilitar a depuração.

#Serviços em Nuvem (Opcional)
Se você quiser executar o pipeline em um ambiente de nuvem, siga estas instruções:

#AWS:

Use o Amazon MWAA (Managed Workflows for Apache Airflow) para executar o DAG.

Configure um bucket no S3 para armazenar os dados.

#GCP:

Use o Cloud Composer para executar o DAG.

Configure um bucket no Cloud Storage para armazenar os dados.

#Azure:

Use o Azure Data Factory ou Azure Databricks para executar o pipeline.

Configure um container no Azure Data Lake Storage para armazenar os dados.
