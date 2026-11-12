# Urban Mobility Analytics

Pipeline de Engenharia de Dados desenvolvido com Python, PySpark, PostgreSQL, dbt e Apache Airflow, utilizando dados públicos de corridas Yellow Taxi da NYC TLC.

## Objetivo

Construir um pipeline de dados para ingerir, validar, transformar e disponibilizar indicadores analíticos referentes às corridas de táxi de Nova York, no período de janeiro a junho de 2025.

## Tecnologias

- Python
- PySpark
- Apache Airflow 3.4
- PostgreSQL 18
- dbt Core
- dbt-postgres
- Docker
- PyArrow
- Parquet

## Arquitetura

O projeto utiliza a arquitetura Medalhão:

- **Bronze:** arquivos Parquet originais.
- **Silver:** dados padronizados, classificados e validados com PySpark.
- **Gold:** modelos analíticos construídos com dbt.

## Fluxo de processamento

1. Download incremental dos arquivos mensais.
2. Auditoria dos arquivos Bronze.
3. Processamento e classificação com PySpark.
4. Separação de registros válidos e registros em quarentena.
5. Carga da Silver no PostgreSQL.
6. Modelagem analítica com dbt.
7. Disponibilização dos indicadores na Gold.

A orquestração é realizada pelo Airflow por meio da DAG `urban_mobility_pipeline`.

## Regras de qualidade

Registros são enviados para quarentena quando apresentam:
- Data de embarque nula.
- Data de desembarque nula.
- Desembarque anterior ou igual ao embarque.
- Data de embarque fora do mês de referência.

Registros sem rejeições, mas com anomalias, recebem status `REVIEW`, incluindo:
- Distância menor ou igual a zero.
- Tarifa menor ou igual a zero.
- Tipo de pagamento nulo, igual a zero ou fora da referência.

Os demais registros recebem status `APPROVED`.

## Regra de receita

Somente os tipos de pagamento 1 (Credit card) e 2 (Cash) são elegíveis para receita.

A receita reconhecida utiliza o campo `total_amount` dos pagamentos elegíveis.

## Modelagem analítica

A camada Gold contém:
- `dim_payment_types`: dimensão de tipos de pagamento.
- `fct_monthly_vendor_performance`: indicadores mensais por fornecedor.
- `mv_monthly_vendor_performance`: visão materializada dos indicadores.

Indicadores:
- Total de viagens.
- Receita reconhecida.
- Quantidade de viagens elegíveis para receita.
- Tarifa média.
- Distância média.
- Duração média das viagens.

## Resultados do processamento

Período: janeiro a junho de 2025.

| Indicador | Resultado |
|---|---:|
| Arquivos mensais Bronze | 6 |
| Registros Bronze | 24.083.384 |
| Registros Silver | 23.886.128 |
| Registros em quarentena | 197.256 |
| Registros em revisão | 6.039.676 |
| Registros aprovados | 17.846.452 |
| Registros Gold | 18 |

Receita reconhecida total: 508.044.416,17.

A execução atual do dbt apresentou 14 operações concluídas com sucesso, incluindo 8 testes de qualidade, sem erros ou avisos.

## Estrutura do projeto

- `dags/`: DAG do Airflow.
- `src/ingestion/`: download dos arquivos de origem.
- `src/silver/`: transformação e carga da Silver.
- `src/common/`: conexão e recursos compartilhados.
- `dbt/models/`: modelos analíticos.
- `dbt/seeds/`: dados de referência.
- `dbt/tests/`: testes de qualidade.
- `docs/`: documentação técnica.
- `sql/`: scripts SQL.
- `data/`: arquivos de dados locais, não versionados.

## Configuração

Consulte `docs/execution_guide.md` para instalação, configuração e execução.

Crie o arquivo `.env` a partir de `.env.example` e configure as credenciais do PostgreSQL.

Não compartilhe credenciais nem versione o arquivo `.env`.

## Dados

Fonte: NYC Taxi and Limousine Commission (TLC).

Portal oficial: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Os arquivos Parquet não são incluídos no repositório Git.

## Observação sobre validação

Os seis meses de dados (janeiro a junho de 2025) foram processados na camada Silver. A carga no PostgreSQL foi concluída e validada, totalizando 23.886.128 registros. A execução do dbt foi concluída com 14 operações aprovadas, 8 testes de qualidade e nenhum erro ou aviso. A DAG do Airflow foi reconhecida e validada sem erros de importação. A execução completa da DAG de ponta a ponta ainda não foi realizada nesta validação final.
