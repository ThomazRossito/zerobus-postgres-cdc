# PostgreSQL local → Databricks em tempo real, sem Kafka: Debezium Server + Zerobus Ingest

Demo do artigo/vídeo "Zerobus Ingest": um PostgreSQL rodando no MacBook (atrás do NAT de casa, inalcançável
por qualquer conector que *puxe* dados) empurra cada insert/update/delete para tabelas Delta no Unity Catalog.
Nenhum broker no meio. O Debezium Server lê o WAL e entrega direto no Zerobus pelo sink nativo (3.7+).

```
 MacBook (rede de casa)                                  Azure Databricks
 ┌──────────────┐  WAL (pgoutput)   ┌────────────────────┐  gRPC/TLS  ┌──────────────────┐
 │ PostgreSQL   │ ────────────────▶ │ Debezium Server    │ ─────────▶ │ Zerobus Ingest   │
 │ retail.*     │ slot zerobus_demo │ sink=zerobus       │ porta 443  │ ▶ retail.*       │
 └──────────────┘                   │ unwrap SMT (typed) │            │   bronze Delta   │
        ▲                           └────────────────────┘            └────────┬─────────┘
 gerador Python: COPY + simulação (insert/update/delete)                       │ consultável em ~5 s
                                             Lakeflow AUTO CDC (próximo passo) ▼
                                                                      ┌──────────────────┐
                                                                      │ retail_silver.*  │
                                                                      └──────────────────┘
```

Por que Zerobus e não o conector gerenciado do Lakeflow Connect: o conector gerenciado **puxa** do banco por um
gateway de ingestão que precisa alcançar o Postgres. Um laptop em rede doméstica não é alcançável; ele só consegue
**empurrar**. É o mesmo caso das fábricas da Joby Aviation e da Toyota, que a Databricks publicou como casos de Zerobus. Quando o banco está
numa rede alcançável, o conector gerenciado é a escolha simples.

## Resultados medidos (30/10/2026)

MacBook Pro Apple Silicon (10 núcleos, 32 GB), PostgreSQL 18, Debezium Server 3.7.0.Final, Azure Databricks eastus,
rede doméstica. Não é benchmark: é o que você deve ver se repetir.

| Medida | Resultado |
|---|---|
| Snapshot de 10 GB (94,2 milhões de linhas) com `run-parallel.sh 8` | 14 min 47 s, 106 mil linhas/s, contando a partida das JVMs |
| Uma instância, lote padrão (2048) | 4,5 mil linhas/s |
| Uma instância, `max.batch.size=51200` | 26,3 mil linhas/s |
| 4 e 8 instâncias em paralelo (2,2 milhões de linhas) | 96,9 mil e 109,5 mil linhas/s |
| CDC, commit no Postgres até a linha consultável | p50 ~5 s, p95 7,7 s |
| Só a captura, commit até o Debezium | p50 0,25 s |
| Duplicatas de order_id no snapshot (`__op = 'r'`), nos testes de 2,2 milhões e na carga de 10 GB | 0 |

Com 8 instâncias, o limite foi o laptop (CPU em 99%, memória em alerta), não o Zerobus.

## O que está verificado e o que não está

| Item | Status |
|---|---|
| Scripts SQL do Postgres (replicação, schema, publicação, REPLICA IDENTITY FULL) | Testados em PostgreSQL 16 e 18 |
| `seed_bulk.py` (100 mil pedidos em ~2 s via COPY) e `simulate_ops.py` | Testados; `pgoutput` emite os eventos da publicação |
| Nomes e defaults das propriedades do sink (`debezium.sink.zerobus.*`) | Conferidos no código-fonte e na documentação do Debezium Server 3.7.0.Final |
| Mapa de tipos Debezium → Delta (timestamp→TIMESTAMP, numeric→DECIMAL, timestamptz→STRING) | Documentação oficial do sink |
| Requisitos do Zerobus (tabela existente, grants diretos na tabela, sem CDF, regiões Azure incl. brazilsouth) | Documentação Databricks de 19 a 22/10/2026 |
| Debezium Server rodando contra o Zerobus de verdade | Testado de ponta a ponta em 30/10/2026 (PostgreSQL 18, Azure Databricks eastus) |
| `measure/latency.py` (databricks-sdk, statement execution) | Testado contra o workspace em 30/10/2026 |
| Silver com AUTO CDC (`databricks/03_pipeline_silver.sql`) | **Ainda não executada** neste projeto |

## Pré-requisitos no Mac

PostgreSQL 14+ já instalado e rodando; Anaconda ou Miniconda; Java 21+ (`brew install openjdk@21`); Databricks CLI autenticada
(`databricks auth login`) para o script de latência; acesso a um workspace Azure Databricks em uma das regiões com
Zerobus (lista oficial inclui `brazilsouth`, `eastus`, `eastus2`, `centralus`, `westeurope` e outras).

```bash
conda create -n bricks_zerobus python=3.12 -y
conda activate bricks_zerobus
pip install -r requirements.txt     # psycopg, tqdm e databricks-sdk
cp .env.example .env                # preencha
```

Ou, de uma vez, com o `environment.yml` da raiz: `conda env create -f environment.yml && conda activate bricks_zerobus`.

## Passo a passo

### 1. Postgres: replicação lógica e schema

```bash
psql -U postgres -d postgres -f postgres/01_replication.sql   # wal_level=logical + usuário dbz
# reinicie o Postgres (brew services restart postgresql@18  ou  Postgres.app)
psql -U postgres -c "SHOW wal_level;"                          # deve ser 'logical'
psql -U postgres -c "CREATE DATABASE retail_demo;"
psql -U postgres -d retail_demo -f postgres/02_schema_retail.sql
```

### 2. Carga em massa

```bash
python generator/seed_bulk.py --stores 40 --products 400 --orders 200000
```

Cria lojas, produtos, estoque (lojas × produtos) e 200 mil pedidos históricos. Rode antes de subir o Debezium se
quiser ver o **snapshot inicial** entrando no Databricks (`__op = 'r'`); rode depois se quiser ver só o streaming.

Rodar de novo faz **append**: lojas, produtos e estoque ficam como estão e entram só pedidos novos, com `order_id`
seguindo a sequência (`python generator/seed_bulk.py --orders 5000`). Com o Debezium no ar, cada pedido chega na
bronze como `__op = 'c'`. O `--truncate` recomeça do zero, mas só com o Debezium parado: o conector do Postgres ignora
TRUNCATE por padrão (`skipped.operations=t`), a bronze não é limpa e os `order_id` voltam para 1.

### 3. Databricks: tabelas e permissões

No SQL Editor, com o seu catálogo:

1. `databricks/01_tables.sql`: cria o schema `retail` e as tabelas bronze `orders` e `inventory` com o contrato do modo
   `typed` (colunas do Postgres + `__op`, `__ts_ms`, `__source_ts_ms`, `__lsn`, `__deleted`).
2. Crie o service principal (Settings → Identity and Access), gere client ID e secret, marque **Workspace access**.
3. `databricks/02_grants.sql`: `USE CATALOG`, `USE SCHEMA`, `SELECT` e `MODIFY` **direto nas tabelas**.

Monte o `ZEROBUS_SERVER_ENDPOINT`: `<workspace-id>.zerobus.<região>.azuredatabricks.net`, em que workspace-id é o
número depois de `adb-` na URL do workspace (ou do `?o=`) e região é a do workspace (ex.: `brazilsouth`).

### 4. Debezium Server

```bash
./debezium-server/run.sh
```

O script baixa a distribuição `3.7.0.Final` do Maven Central, copia `application.properties` e sobe o processo lendo o
`.env`. O primeiro run faz o snapshot das duas tabelas (`snapshot.mode=initial`) e depois fica em streaming.
Log esperado: `Zerobus gRPC sink connected`, depois linhas de métricas a cada 20 lotes (`metrics.log.interval=20`).

Como a tabela de destino é resolvida: o Debezium nomeia cada evento como `<topic.prefix>.<schema>.<tabela>`.
Com `topic.prefix = UC_CATALOG`, `retail.orders` vira `<catálogo>.retail.orders` e o sink grava direto nela, uma stream
por tabela. Não precisa de roteamento.

### 5. Operação em tempo real

Em outro terminal:

```bash
python generator/simulate_ops.py --rate 20 --minutes 10
```

Pedidos novos (`c`), mudanças de status (`u`), cancelamentos como delete físico (`d`, chega como `__deleted='true'`
com a linha inteira, graças ao `REPLICA IDENTITY FULL`) e ajustes de estoque. Cada operação é registrada em
`generator/events.csv` com o timestamp do commit.

### 6. Silver com Lakeflow Spark Declarative Pipelines

Crie um pipeline serverless com `databricks/03_pipeline_silver.sql` como fonte, catálogo = o seu, schema padrão
`retail_silver`. `AUTO CDC INTO ... SEQUENCE BY __lsn ... APPLY AS DELETE WHEN __deleted = 'true'` transforma o log
de eventos em estado atual. O LSN como sequência é o que absorve as duplicatas do at-least-once.

### 7. Medir a latência (o número do artigo)

```bash
python measure/latency.py --minutes 10      # em paralelo com o simulate_ops
```

Consulta a bronze a cada 2 s e anota quando cada evento do `events.csv` ficou consultável. Sai p50/p95/max em
`measure/latency.csv`. A resolução é o intervalo de polling, então a latência real é igual ou menor que a medida.
Precisa de `UC_CATALOG` e `WAREHOUSE_ID` no `.env`. Só conta eventos gravados depois que o script começou (linhas
antigas do `events.csv` são ignoradas) e, quando um pedido tem vários updates, mede só o primeiro.
Para o artigo, rode com `--poll 1` por uns 10 minutos. O resultado já vai do commit no Postgres até a linha consultável;
a query 3 de `databricks/04_checks.sql` mostra só o trecho da captura (do commit até o Debezium).

### 8. Conferências

`databricks/04_checks.sql`: eventos por operação, frescor, duplicatas por (order_id, __lsn), bronze × silver, tipos.

### 9. Carga grande: snapshot em paralelo (`run-parallel.sh`)

Um stream do Zerobus, saindo do Mac para eastus, chegou a ~26 mil linhas/s com lote de 51200, usando ~1 núcleo de
CPU. Com uma instância, o limite é o stream, não o laptop. O `run-parallel.sh` sobe N instâncias do Debezium Server, cada uma lendo uma
faixa de `order_id` (`snapshot.select.statement.overrides`) com `snapshot.mode=initial_only`, e todas gravam na mesma
tabela Delta, cada uma com o seu stream.

```bash
# 0) Debezium normal parado; slot e offsets apagados antes de qualquer carga grande no Postgres
#    SELECT pg_drop_replication_slot('zerobus_demo');   e   rm debezium-server/dist/debezium-server/data/offsets.dat
# 1) (opcional) ~10 GB: 114 bytes/linha na tabela de pedidos => ~94 milhões de linhas
python generator/seed_bulk.py --orders 92000000
# 2) Databricks: TRUNCATE TABLE <catalogo>.retail.orders; TRUNCATE TABLE <catalogo>.retail.inventory;
# 3) snapshot em paralelo (8 instâncias na medição; heap de 1,5 GB por instância)
./debezium-server/run-parallel.sh 8
# 4) seguir com o CDC a partir de agora, sem refazer o snapshot
DEBEZIUM_SOURCE_SNAPSHOT_MODE=no_data ./debezium-server/run.sh
```

- Ninguém escreve no Postgres entre o passo 3 e o 4: as faixas e as contagens são calculadas no início, e o CDC
  começa a contar do momento em que o passo 4 cria o slot.
- `initial_only` não cria slot de replicação e não derruba o processo no fim. O script acompanha o `r=` do log de
  métricas do sink (contado depois do flush) e para cada instância quando ela entregou todas as linhas da faixa.
- Cada instância tem pasta própria em `debezium-server/parallel/iN` (config, offsets, `debezium.log`) e porta HTTP
  própria (8090, 8091...). A instância 0 leva também `retail.inventory`.
- Medição: a mesma consulta `__op = 'r'` (linhas, segundos, linhas/s) no Databricks.
- Testado em 30/10/2026: com 2,2 milhões de linhas, 4 instâncias deram 96,9 mil linhas/s e 8 deram 109,5 mil; com 8,
  os 10 GB levaram 14 min 47 s. De 4 para 8 o ganho foi pequeno porque o laptop chegou ao limite (CPU em 99%,
  memória em alerta).
- O seed de 92 milhões de linhas levou 22 min: 7 min 50 s de envio e o resto com o Postgres conferindo as chaves
  estrangeiras no fim do `COPY`. A opção `--sem-fk` pula essa conferência (precisa de superusuário; ainda não testada).

## Decisões de desenho e armadilhas

**`timestamp` sem fuso, em UTC.** O sink mapeia `timestamptz` para `STRING` ISO-8601 e `timestamp` para `TIMESTAMP`.
Se o seu banco real usa `timestamptz`, a bronze recebe string e a conversão fica para a silver.

**Modo `typed` com o SMT `ExtractNewRecordState`.** É a recomendação da própria documentação do sink (JSON plano).
O modo `envelope` guarda o evento cru (`operation`, `key`, `value`, `idempotency_key`) e é a alternativa quando se quer
uma bronze fiel ao envelope; nesse modo **não** se aplica o unwrap. Os dois modos são contratos de tabela diferentes.

**Lote grande.** O sink manda o lote, faz flush e espera o ack do Zerobus antes de pedir o próximo. Com o padrão de
2048 linhas, uma instância fez 4,5 mil linhas/s; com `max.batch.size=51200`, 26 mil. Com pouco volume o lote não
enche e sai a cada `poll.interval.ms` (500 ms), então a latência do CDC não muda.

**O Zerobus nunca cria nem altera tabela.** Campo que não existe na tabela é rejeitado, não descartado em silêncio.
Mudou o schema no Postgres? Altere a tabela Delta antes, ou o Debezium para com erro de schema.

**At-least-once.** Um lote que falha no meio pode reenviar eventos já duráveis. Na bronze aparecem duplicatas com o
mesmo `__lsn`; a silver com `SEQUENCE BY __lsn` não se altera. Nunca anunciar exactly-once.

**Durável ≠ consultável.** O ack do Zerobus (~150 ms) diz que o dado está seguro; a linha aparece na consulta em ~5 s,
porque a materialização em Delta é um passo separado.

**Grants.** `ALL PRIVILEGES` no schema não serve; `MODIFY` e `SELECT` precisam estar na tabela. O erro 4024 /
`authorization_details` é isso.

**Offsets.** Ficam em `debezium-server/dist/debezium-server/data/offsets.dat`. Apague junto com o slot
(`SELECT pg_drop_replication_slot('zerobus_demo')`) para refazer o snapshot do zero.

**Slot de replicação órfão.** Se o Debezium ficar parado, o slot segura WAL e o disco do Mac cresce. Para pausar por
dias, derrube o slot.

## Estrutura

```
.env.example                   variáveis (Postgres, Databricks, Zerobus, warehouse)
environment.yml                env conda bricks_zerobus (psycopg, tqdm, databricks-sdk)
requirements.txt               as mesmas dependências, para pip
postgres/01_replication.sql    wal_level=logical, usuário dbz
postgres/02_schema_retail.sql  schema retail, REPLICA IDENTITY FULL, publicação zerobus_pub
generator/seed_bulk.py         carga em massa via COPY
generator/simulate_ops.py      operação contínua + events.csv
debezium-server/application.properties  source Postgres + sink Zerobus (typed) + unwrap
debezium-server/run.sh         baixa 3.7.0.Final e sobe
debezium-server/run-parallel.sh  snapshot em paralelo: N instâncias, uma faixa de order_id cada
databricks/01_tables.sql       bronze (contrato typed)
databricks/02_grants.sql       service principal
databricks/03_pipeline_silver.sql  SDP com AUTO CDC
databricks/04_checks.sql       conferências e métricas
measure/latency.py             latência commit → consultável
LICENSE                        Apache 2.0
```

## Fontes

Abertas em 31/10/2026:

- [Zerobus Ingest, visão geral](https://learn.microsoft.com/en-us/azure/databricks/ingestion/zerobus-overview)
- [Use Zerobus Ingest](https://learn.microsoft.com/en-us/azure/databricks/ingestion/zerobus-ingest)
- [Cotas do Zerobus Ingest](https://learn.microsoft.com/en-us/azure/databricks/ingestion/zerobus-limits)
- [Debezium Server](https://debezium.io/documentation/reference/stable/operations/debezium-server.html)
- [Conector PostgreSQL do Debezium](https://debezium.io/documentation/reference/connectors/postgresql)
- [Debezium 3.7.0.Beta1](https://debezium.io/blog/2026/08/26/debezium-3-7-beta1-released/)
- [Código do sink, tag v3.7.0.Final](https://github.com/debezium/debezium-server/tree/v3.7.0.Final/debezium-server-databricks-zerobus)

Consultadas em 29/10/2026:


- Databricks, *Use the Zerobus Ingest connector*, *Zerobus quotas*, *Zerobus limits*, *Zerobus release stages*
  (gRPC, REST e OTLP GA; Kafka e MQTT Beta; página de 22/10/2026) e *Feature region support* (Azure, 21/10/2026).
- Debezium Server 3.7, seção *Databricks Zerobus Ingest* (rotas `zerobus`, `zerobus-rest` e `kafka`; propriedades,
  contrato do envelope, mapa de tipos) e código-fonte do módulo `debezium-server-databricks-zerobus`.
- Debezium, *Event flattening* (`ExtractNewRecordState`, `delete.tombstone.handling.mode=rewrite`, `add.fields`).
- Databricks, *Lakeflow Spark Declarative Pipelines*, sintaxe `AUTO CDC INTO`.
