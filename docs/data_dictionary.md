# Dicionário de Dados — Urban Mobility Analytics

## 1. Origem

NYC TLC Yellow Taxi Trip Records — janeiro a junho de 2025.

Os arquivos de origem são Parquet mensais. Os campos abaixo representam os dados originais e os metadados acrescentados pelo processamento.

## 2. Campos da Silver

| Campo | Descrição |
|---|---|
| VendorID | Identificador do fornecedor do sistema de registro |
| tpep_pickup_datetime | Data e hora de embarque |
| tpep_dropoff_datetime | Data e hora de desembarque |
| passenger_count | Quantidade de passageiros |
| trip_distance | Distância da viagem em milhas |
| RatecodeID | Código da tarifa aplicada |
| store_and_fwd_flag | Indicador de armazenamento e encaminhamento |
| PULocationID | Identificador da zona de embarque |
| DOLocationID | Identificador da zona de desembarque |
| payment_type | Código do tipo de pagamento |
| fare_amount | Valor da tarifa |
| extra | Valores adicionais |
| mta_tax | Taxa MTA |
| tip_amount | Valor da gorjeta |
| tolls_amount | Valor dos pedágios |
| improvement_surcharge | Taxa de melhoria |
| total_amount | Valor total registrado para a viagem |
| congestion_surcharge | Taxa de congestionamento |
| Airport_fee | Taxa aeroportuária |
| cbd_congestion_fee | Taxa de congestionamento da região central |
| rejection_reasons | Motivos que direcionam o registro à quarentena |
| quality_flags | Sinalizadores de anomalias para revisão |
| reference_month | Mês de referência no formato YYYY-MM |
| source_file | Nome do arquivo Parquet de origem |
| is_valid_revenue_payment | Indica se o pagamento é elegível para receita |
| quality_status | Classificação de qualidade do registro |

## 3. Classificação de qualidade

| Status | Significado |
|---|---|
| APPROVED | Registro sem rejeições e sem sinalizadores de anomalia |
| REVIEW | Registro sem rejeições, mas com sinalizadores de anomalia |
| QUARANTINE | Registro com um ou mais motivos de rejeição |

### Motivos de rejeição

- `PICKUP_DATETIME_NULL`: data de embarque nula.
- `DROPOFF_DATETIME_NULL`: data de desembarque nula.
- `INVALID_TRIP_DURATION`: desembarque anterior ou igual ao embarque.
- `PICKUP_OUTSIDE_REFERENCE_MONTH`: embarque fora do mês de referência.

### Sinalizadores de revisão

- `NON_POSITIVE_TRIP_DISTANCE`: distância menor ou igual a zero.
- `NON_POSITIVE_FARE`: tarifa menor ou igual a zero.
- `PAYMENT_TYPE_ZERO`: tipo de pagamento igual a zero.
- `PAYMENT_TYPE_NULL`: tipo de pagamento nulo.
- `PAYMENT_TYPE_OUTSIDE_REFERENCE`: tipo de pagamento fora dos códigos de 0 a 6.

## 4. Referência de pagamentos

| Código | Descrição | Elegível para receita |
|---:|---|---|
| 1 | Credit card | Sim |
| 2 | Cash | Sim |
| 3 | No charge | Não |
| 4 | Dispute | Não |
| 5 | Unknown | Não |
| 6 | Voided trip | Não |

A elegibilidade é definida pelo código de pagamento 1 ou 2.

## 5. Modelagem Gold

### fct_monthly_vendor_performance

Grão: uma linha por mês de referência e fornecedor (`VendorID`).

| Campo | Descrição |
|---|---|
| reference_month | Mês de referência |
| vendor_id | Identificador do fornecedor |
| total_trips | Quantidade total de viagens |
| total_revenue | Receita reconhecida de pagamentos elegíveis |
| revenue_eligible_trips | Quantidade de viagens elegíveis para receita |
| average_fare | Tarifa média |
| average_distance | Distância média |
| average_trip_duration_minutes | Duração média da viagem em minutos |

### dim_payment_types

Dimensão de referência dos tipos de pagamento, com `payment_type_id` e `payment_type_name`.

### mv_monthly_vendor_performance

Visão materializada que disponibiliza os indicadores mensais da fato por fornecedor.

## 6. Regras de receita e duração

- `recognized_revenue`: utiliza `total_amount` quando `is_valid_revenue_payment` é verdadeiro; caso contrário, retorna zero.
- `revenue_eligible_trip`: retorna 1 para pagamento elegível e 0 para os demais.
- `trip_duration_minutes`: diferença entre desembarque e embarque convertida em minutos.
