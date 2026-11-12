# Urban Mobility Analytics

## 1. Objetivo

Construir um pipeline de Engenharia de Dados para processar registros públicos de corridas de táxi de Nova York (NYC TLC), utilizando a base Yellow Taxi de 2025.

O pipeline deverá suportar ingestão incremental, tratamento de inconsistências, armazenamento estruturado e disponibilização de indicadores analíticos.

## 2. Arquitetura

A solução utilizará a arquitetura Medalhão:

- Bronze: armazenamento dos arquivos Parquet originais.
- Silver: dados tratados, padronizados e validados.
- Gold: dados modelados para análises de negócio.

## 3. Tecnologias

| Tecnologia | Responsabilidade |
|---|---|
| Python | Ingestão e controle de arquivos |
| PySpark | Processamento e qualidade |
| PostgreSQL | Persistência das camadas |
| dbt | Modelagem analítica |
| Airflow | Orquestração |
| Git | Versionamento |

## 4. Fluxo de dados

1. Identificação dos arquivos mensais disponíveis.
2. Download incremental dos arquivos Yellow Taxi.
3. Armazenamento dos arquivos originais na Bronze.
4. Leitura e processamento com PySpark.
5. Aplicação das regras de qualidade na Silver.
6. Carga dos dados tratados no PostgreSQL.
7. Transformação dos dados com dbt.
8. Disponibilização dos indicadores na Gold.
9. Execução automatizada pelo Airflow.

## 5. Regras de negócio

- A competência mensal será derivada da data de embarque.
- Somente pagamentos válidos serão considerados para cálculo de receita.
- Registros inválidos deverão possuir justificativa.

## 6. Indicadores analíticos

A camada Gold deverá disponibilizar:

- Total de corridas por mês.
- Valor total das corridas válidas.
- Ticket médio por corrida.
- Distância média.

Os indicadores serão agrupados por mês e VendorID.

## 7. Estratégia incremental

O processo deverá identificar os meses já ingeridos e evitar downloads e cargas duplicadas.

A execução deverá permitir reprocessamento controlado dos períodos selecionados.

## 8. Qualidade e rastreabilidade

O pipeline deverá registrar:

- Arquivos processados.
- Período de referência.
- Quantidade de registros.
- Status de processamento.
- Erros encontrados.
- Data e horário da execução.

## 9. Orquestração

O Apache Airflow será responsável por coordenar:

1. Download dos arquivos.
2. Processamento da camada Silver.
3. Processamento da camada Gold.

## 10. Entrega

O código-fonte, documentação e instruções de execução serão disponibilizados em repositório GitHub.
