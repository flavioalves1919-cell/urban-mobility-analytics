
# Guia de Execução — Urban Mobility Analytics

## 1. Pré-requisitos

- Linux ou WSL2 com Ubuntu.
- Python com suporte a ambiente virtual.
- Docker Desktop ou Docker Engine.
- PostgreSQL acessível pelos containers.
- Git.

## 2. Clonar o repositório

```bash
git clone URL_DO_REPOSITORIO
cd data-engineering
```

Substitua URL_DO_REPOSITORIO pelo endereço do repositório GitHub.

## 3. Configurar o ambiente

Crie o arquivo de variáveis:

```bash
cp .env.example .env
```

Preencha o `.env` com as credenciais do PostgreSQL. Nunca envie esse arquivo ao GitHub.

Configure também `~/.dbt/profiles.yml` com o perfil `urban_mobility_dbt`, apontando para o banco `urban_mobility`, usuário, senha, host e schema `gold`.

## 4. Construir as imagens

Na raiz do projeto:

```bash
docker build -f Dockerfile.spark -t urban-mobility-spark:1.0 .
docker build -f Dockerfile.loader -t urban-mobility-loader:1.0 .
docker build -f Dockerfile.dbt -t urban-mobility-dbt:1.0 .
```

## 5. Iniciar o Airflow

```bash
docker compose up -d
```

Interface: http://localhost:8080

## 6. Executar o pipeline

Na interface do Airflow, localize `urban_mobility_pipeline` e dispare uma execução manual.

Ordem das tarefas:

1. ingest_yellow_taxi
2. audit_bronze
3. process_silver
4. load_silver_postgres
5. run_dbt_gold

A DAG utiliza `schedule=None`, portanto não possui agendamento automático.

## 7. Execução individual

Download Bronze:

```bash
python -m src.ingestion.download_yellow_taxi --year 2025 --start-month 1 --end-month 6
```

Transformação Silver:

```bash
python -m src.silver.transform_yellow_taxi
```

Carga PostgreSQL:

```bash
python -m src.silver.load_silver_postgres --year 2025 --start-month 1 --end-month 6
```

## 8. Executar dbt

```bash
cd dbt
dbt seed --select payment_types
dbt build
```

Testes:

```bash
dbt test
```

## 9. Encerrar

Na raiz do projeto:

```bash
docker compose down
```

## 10. Observações

- Os arquivos Parquet ficam em `data/` e não são versionados.
- As credenciais ficam no `.env`, também excluído do Git.
- O Spark está configurado com `local[1]` e memória de driver de `1g`.
- Endereços, credenciais e configurações de rede devem ser adaptados ao ambiente.
- A execução individual exige dependências instaladas e PostgreSQL configurado.
