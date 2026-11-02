#!/usr/bin/env bash
# Snapshot em paralelo: N instâncias do Debezium Server, cada uma lendo uma faixa de order_id
# e gravando na MESMA tabela Delta por um stream próprio do Zerobus (o sink abre um stream por
# tabela e por processo; a doc do Zerobus diz que abrir mais streams é o jeito de escalar).
#
# Uso (da raiz do projeto):   ./debezium-server/run-parallel.sh [N]        N = instâncias (padrão 4)
# Variáveis opcionais:        BATCH=51200  JAVA_OPTS="-Xms512m -Xmx1536m"  HTTP_BASE=8090  YES=1  ENV_FILE=outro.env
#                             IDLE=180  (segundos sem ack, depois do fim do snapshot, para considerar a instância pronta)
# Medido em 30/10/2026 (Mac 10 núcleos, 32 GB): 8 instâncias deixaram a CPU em 99% e a memória em alerta;
# por isso fila de 2x o lote e heap de 1,5 GB por instância.
#
# Antes de rodar:
#   1) o Debezium "normal" (run.sh) parado; o script confere
#   2) bronze limpa no Databricks: TRUNCATE nas duas tabelas
#   3) ninguém escrevendo no Postgres até o fim (as faixas e as contagens são calculadas no início)
#   4) ./debezium-server/run.sh já rodou uma vez (baixa a distribuição e o jar do unwrap)
#
# Como funciona:
#   - cada instância usa snapshot.mode=initial_only: faz o snapshot e não faz streaming, então
#     não cria slot de replicação (PostgresConnectorTask só cria slot quando o modo faz streaming)
#   - a faixa vem de snapshot.select.statement.overrides.retail.orders
#   - a instância 0 leva também retail.inventory (é pequena)
#   - o initial_only não derruba o processo no fim; o script acompanha o contador r= do log de
#     métricas do sink (contado só depois do flush, ou seja, já durável) e para cada instância
#     quando ela entregou todas as linhas da faixa
#
# Depois: o CDC segue com o run.sh em snapshot.mode=no_data (o script mostra os comandos no fim).
set -euo pipefail

N="${1:-4}"                                    # instâncias = streams do Zerobus em paralelo
BATCH="${BATCH:-51200}"                        # max.batch.size de cada instância
HTTP_BASE="${HTTP_BASE:-8090}"                 # porta HTTP do Quarkus: i0 usa 8090, i1 8091...
JAVA_OPTS="${JAVA_OPTS:--Xms512m -Xmx1536m}"   # heap de cada JVM: multiplique por N para ter o total

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
DIST="$HERE/dist/debezium-server"
WORK="$HERE/parallel"

[[ "$N" =~ ^[0-9]+$ ]] && (( N >= 1 && N <= 16 )) || { echo "N deve ser um número de 1 a 16"; exit 1; }
[[ "$BATCH" =~ ^[0-9]+$ ]] || { echo "BATCH deve ser um número"; exit 1; }

# 1) variáveis do .env
ENV_FILE="${ENV_FILE:-$ROOT/.env}"   # outro arquivo: ENV_FILE=outro.env ./debezium-server/run-parallel.sh
[[ "$ENV_FILE" = /* ]] || ENV_FILE="$ROOT/$ENV_FILE"
if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ENV_FILE"
  set +a
else
  echo "faltou $ENV_FILE (copie de .env.example)"; exit 1
fi
for v in ZEROBUS_SERVER_ENDPOINT DATABRICKS_WORKSPACE_URL DATABRICKS_CLIENT_ID DATABRICKS_CLIENT_SECRET UC_CATALOG PG_HOST PG_PORT PG_USER PG_PASSWORD PG_DBNAME; do
  [[ -n "${!v:-}" ]] || { echo "variável $v vazia em $ENV_FILE"; exit 1; }
done

# 2) Java 21+ (mesma busca do run.sh)
java_major() { "$1" -version 2>&1 | awk -F'"' '/version/ {print $2}' | cut -d. -f1; }
JAVA_BIN=""
cands=("$(command -v java || true)")
if p=$(brew --prefix openjdk@21 2>/dev/null); then cands+=("$p/bin/java"); fi
if p=$(/usr/libexec/java_home -v 21+ 2>/dev/null); then cands+=("$p/bin/java"); fi
for cand in "${cands[@]}"; do
  [[ -n "$cand" && -x "$cand" ]] || continue
  if [[ "$(java_major "$cand")" -ge 21 ]]; then JAVA_BIN="$cand"; break; fi
done
[[ -n "$JAVA_BIN" ]] || { echo "Java 21+ não encontrado. Instale com: brew install openjdk@21"; exit 1; }
JH=$("$JAVA_BIN" -XshowSettings:properties -version 2>&1 | awk -F' = ' '/^ *java.home = / {print $2}')
[[ -x "$JH/bin/java" ]] || { echo "não consegui achar o java.home de $JAVA_BIN"; exit 1; }
export JAVA_HOME="$JH"

# 3) distribuição (o run.sh é quem baixa)
RUNNER=$(ls "$DIST"/debezium-server-*runner.jar 2>/dev/null | head -1 || true)
if [[ -z "$RUNNER" ]] || ! ls "$DIST"/lib/debezium-connect-plugins-*.jar >/dev/null 2>&1; then
  echo "distribuição incompleta em $DIST. Rode ./debezium-server/run.sh uma vez e pare com Ctrl+C."; exit 1
fi

# 4) nada de outro Debezium Server rodando: ele gravaria nas mesmas tabelas e a conta das faixas não bateria
if pgrep -f 'io[.]debezium[.]server[.]Main$' >/dev/null 2>&1; then
  echo "já existe um Debezium Server rodando (pid $(pgrep -f 'io[.]debezium[.]server[.]Main$' | tr '\n' ' ')). Pare antes."; exit 1
fi

# consulta de um valor (ou uma linha com colunas separadas por |) no Postgres.
# Usa o psql se estiver instalado; se não, o psycopg do ambiente Python do projeto.
pgq() {
  if command -v psql >/dev/null 2>&1; then
    PGPASSWORD="$PG_PASSWORD" psql -X -A -t -q -P pager=off -v ON_ERROR_STOP=1 \
      -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DBNAME" -c "$1"
  else
    python3 - "$1" <<'PY'
import os, sys
import psycopg
with psycopg.connect(host=os.environ["PG_HOST"], port=os.environ["PG_PORT"], dbname=os.environ["PG_DBNAME"],
                     user=os.environ["PG_USER"], password=os.environ["PG_PASSWORD"]) as c:
    row = c.execute(sys.argv[1]).fetchone()
    print("|".join("" if v is None else str(v) for v in row))
PY
  fi
}

# 5) faixas de order_id e quantas linhas cada instância tem que entregar
#    B[0..N] são as fronteiras: o intervalo min..max de order_id dividido em N partes iguais.
#    Buraco na sequência (ex.: pedidos apagados) deixa uma faixa com menos linhas; por isso a
#    contagem exata de cada faixa vem numa consulta logo abaixo.
echo "calculando as faixas de order_id..."
IFS='|' read -r LO HI INV <<<"$(pgq "SELECT min(order_id), max(order_id), (SELECT count(*) FROM retail.inventory) FROM retail.orders")"
[[ -n "${LO:-}" && -n "${HI:-}" ]] || { echo "retail.orders está vazia"; exit 1; }
SPAN=$(( HI - LO + 1 ))
B=()
for (( k = 0; k < N; k++ )); do B[k]=$(( LO + k * SPAN / N )); done
B[N]=$(( HI + 1 ))

# uma consulta só conta as N faixas de uma vez: um count(*) FILTER (WHERE ...) por faixa
sel=""
for (( k = 0; k < N; k++ )); do
  sel="${sel}${sel:+, }count(*) FILTER (WHERE order_id >= ${B[k]} AND order_id < ${B[k+1]})"
done
IFS='|' read -r -a CNT <<<"$(pgq "SELECT $sel FROM retail.orders")"

# EXPECT[k] = linhas que a instância k precisa entregar; a i0 leva também o estoque inteiro
EXPECT=(); TOTAL=0
for (( k = 0; k < N; k++ )); do
  EXPECT[k]=${CNT[k]}
  (( k == 0 )) && EXPECT[k]=$(( EXPECT[k] + INV ))
  TOTAL=$(( TOTAL + EXPECT[k] ))
done

# o slot do CDC normal: o snapshot paralelo não usa, mas se ele existir continua segurando WAL
SLOT=$(pgq "SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'zerobus_demo'" || echo 0)

echo
echo "Snapshot em paralelo: $N instâncias, lote de $BATCH, Debezium -> Zerobus ($ZEROBUS_SERVER_ENDPOINT)"
for (( k = 0; k < N; k++ )); do
  extra=""; (( k == 0 )) && extra=" + $INV de estoque"
  echo "  i$k  order_id ${B[k]} até $(( B[k+1] - 1 ))  ->  ${CNT[k]} pedidos${extra}  (porta $(( HTTP_BASE + k )))"
done
echo "  total: $TOTAL linhas"
echo
echo "Confira antes de seguir:"
echo "  - bronze limpa no Databricks (TRUNCATE TABLE ${UC_CATALOG}.retail.orders e ${UC_CATALOG}.retail.inventory)"
echo "  - ninguém escrevendo no Postgres até o fim"
if [[ "${SLOT:-0}" != "0" ]]; then
  echo "  - o slot zerobus_demo existe. Não atrapalha o snapshot, mas segura WAL. Antes de seguir com o CDC"
  echo "    você vai apagar ele (os comandos aparecem no fim)."
fi
if [[ "${YES:-0}" != "1" ]]; then
  read -r -p "Enter para começar, Ctrl+C para sair. " _
fi

# 6) uma pasta por instância: config/application.properties próprio, data/ própria
#    OVR lista as chaves que este script redefine. O grep -Ev tira essas chaves do application.properties
#    base, e o bloco abaixo acrescenta os valores da instância: nenhuma chave aparece duas vezes.
OVR='debezium\.source\.snapshot\.mode|debezium\.source\.slot\.name|debezium\.source\.table\.include\.list|debezium\.source\.snapshot\.select\.statement\.overrides[^=]*|debezium\.source\.offset\.storage\.file\.filename|debezium\.source\.max\.batch\.size|debezium\.source\.max\.queue\.size|debezium\.source\.snapshot\.fetch\.size|debezium\.sink\.zerobus\.max\.inflight\.records|debezium\.sink\.zerobus\.metrics\.log\.interval|quarkus\.http\.port'
rm -rf "$WORK"
mkdir -p "$WORK"
for (( k = 0; k < N; k++ )); do
  d="$WORK/i$k"
  mkdir -p "$d/config" "$d/data"
  tables="retail.orders"; (( k == 0 )) && tables="retail.orders,retail.inventory"
  # registros sem ack em voo: 2x o lote, com piso de 50000
  inflight=$(( BATCH > 50000 ? BATCH * 2 : 50000 ))
  {
    grep -Ev "^[[:space:]]*($OVR)[[:space:]]*=" "$HERE/application.properties" || true
    echo
    echo "# --- snapshot paralelo: instância $k de $N (gerado por run-parallel.sh) ---"
    # só snapshot, sem streaming; o nome de slot é único por instância, mas o slot nem chega a ser criado
    echo "debezium.source.snapshot.mode=initial_only"
    echo "debezium.source.slot.name=zerobus_par_$k"
    echo "debezium.source.table.include.list=$tables"
    # a faixa: no snapshot, o SELECT * FROM retail.orders vira este SELECT com WHERE
    echo "debezium.source.snapshot.select.statement.overrides=retail.orders"
    echo "debezium.source.snapshot.select.statement.overrides.retail.orders=SELECT * FROM retail.orders WHERE order_id >= ${B[k]} AND order_id < ${B[k+1]}"
    # relativo à pasta da instância: cada uma tem o seu offsets.dat
    echo "debezium.source.offset.storage.file.filename=data/offsets.dat"
    echo "debezium.source.max.batch.size=$BATCH"
    echo "debezium.source.max.queue.size=$(( BATCH * 2 ))"
    echo "debezium.source.snapshot.fetch.size=20480"
    echo "debezium.sink.zerobus.max.inflight.records=$inflight"
    # métrica a cada lote: é dela que o passo 8 lê o r=
    echo "debezium.sink.zerobus.metrics.log.interval=1"
    # cada JVM precisa da sua porta HTTP; sem isso todas tentariam a mesma
    echo "quarkus.http.port=$(( HTTP_BASE + k ))"
  } > "$d/config/application.properties"
done

# 7) sobe as instâncias (mesmo comando do run.sh da distribuição, com o diretório de trabalho de cada uma)
export KUBERNETES_AUTH_TRYKUBECONFIG=false
export KUBECONFIG=/dev/null
PIDS=()
cleanup() {
  for p in "${PIDS[@]:-}"; do
    [[ -n "$p" ]] && kill "$p" 2>/dev/null || true
  done
}
# Ctrl+C derruba todas as JVMs juntas: nenhuma instância fica órfã gravando no Zerobus
trap 'echo; echo "interrompido: parando as instâncias"; cleanup; exit 130' INT TERM

echo "usando Java $(java_major "$JH/bin/java"): $JH"
for (( k = 0; k < N; k++ )); do
  d="$WORK/i$k"
  # o cd faz cada JVM ler o config/ e gravar o data/ da própria pasta; $JAVA_OPTS sem aspas de propósito
  # shellcheck disable=SC2086
  ( cd "$d" && exec "$JH/bin/java" $JAVA_OPTS -cp "$RUNNER:$DIST/lib/*" io.debezium.server.Main ) > "$d/debezium.log" 2>&1 &
  PIDS[k]=$!
  echo "  i$k pid ${PIDS[k]}  log: debezium-server/parallel/i$k/debezium.log"
done
echo

# 8) acompanha: r= do log de métricas do sink (já durável) contra o esperado de cada instância
last_r() {
  { grep 'Zerobus sink metrics' "$1" 2>/dev/null || true; } | tail -1 | { grep -o ' r=[0-9]*' || true; } | cut -d= -f2
}
# maior tempo de flush (ms) já visto no log: um lote de 51200 linhas numa rede lenta leva mais de 1 min para
# receber o ack, e nesse intervalo o r= não muda. Antes o script tratava 60 s parado como "terminou" e matava
# a instância no meio do flush (perdeu linhas em 01/11/2026). Agora espera pelo menos IDLE s e 3x o maior flush.
max_flush_s() {
  local ms
  ms=$({ grep 'Zerobus sink metrics' "$1" 2>/dev/null || true; } | tail -1 | { grep -o 'maxFlushMs=[0-9]*' || true; } | cut -d= -f2)
  echo $(( ${ms:-0} / 1000 ))
}
IDLE="${IDLE:-180}"
FIN=(); LASTR=(); LASTCHG=(); ENDS=()
T0=$(date +%s)
for (( k = 0; k < N; k++ )); do FIN[k]=0; LASTR[k]=0; LASTCHG[k]=$T0; ENDS[k]=0; done
T_END=$T0
# A cada 2 s, para cada instância ainda aberta:
#   r >= esperado                                  -> entregou tudo: manda TERM (desligamento limpo)
#   processo morreu antes                          -> erro: mostra o fim do log e para todas
#   snapshot acabou, r parado há IDLE s e há 3x o maior flush -> para com aviso (conferir no Databricks)
#   senão                                          -> continua esperando
while :; do
  now=$(date +%s); el=$(( now - T0 )); tot=0; pend=0; pmin=100; pmax=0
  for (( k = 0; k < N; k++ )); do
    log="$WORK/i$k/debezium.log"
    r=$(last_r "$log"); r=${r:-0}
    if [[ "$r" != "${LASTR[k]}" ]]; then LASTR[k]=$r; LASTCHG[k]=$now; fi
    tot=$(( tot + r ))
    if (( FIN[k] == 0 )); then
      if (( r >= EXPECT[k] )); then
        FIN[k]=1; ENDS[k]=$el; T_END=$now
        kill -TERM "${PIDS[k]}" 2>/dev/null || true
      elif ! kill -0 "${PIDS[k]}" 2>/dev/null; then
        echo; echo "❌ a instância i$k parou antes de terminar. Últimas linhas do log:"
        tail -n 40 "$log"; cleanup; exit 1
      elif grep -q 'Streaming is disabled for snapshot mode' "$log" 2>/dev/null && (( now - LASTCHG[k] >= IDLE )) && (( now - LASTCHG[k] >= 3 * $(max_flush_s "$log") )); then
        echo; echo "⚠️  i$k terminou o snapshot com r=$r, esperado ${EXPECT[k]}, e o sink ficou $(( now - LASTCHG[k] )) s sem novo ack. Parando; confira no Databricks."
        FIN[k]=1; ENDS[k]=$el; T_END=$now
        kill -TERM "${PIDS[k]}" 2>/dev/null || true
      else
        pend=1
      fi
    fi
    den=${EXPECT[k]}; (( den > 0 )) || den=1
    pct=$(( r * 100 / den ))
    (( pct < pmin )) && pmin=$pct
    (( pct > pmax )) && pmax=$pct
  done
  # bash 3.2 (o do macOS) avalia os dois lados do ?: ; por isso nada de divisão dentro do ternário
  rate=0; (( el > 0 )) && rate=$(( tot / el ))
  # linha curta (cabe em 80 colunas): com uma porcentagem por instância ela quebrava e o \r embaralhava
  printf '\r[%5ds] %3d%%  %d de %d  |  %d linhas/s  |  instâncias %d%%-%d%%   ' \
    "$el" "$(( tot * 100 / TOTAL ))" "$tot" "$TOTAL" "$rate" "$pmin" "$pmax"
  (( pend == 0 )) && break
  sleep 2
done
echo
echo "esperando as instâncias fecharem os streams..."
for p in "${PIDS[@]}"; do wait "$p" 2>/dev/null || true; done
trap - INT TERM

EL=$(( T_END - T0 )); (( EL > 0 )) || EL=1
echo
echo "✅ snapshot paralelo concluído: $TOTAL linhas em ${EL}s, $(( TOTAL / EL )) linhas/s com $N streams"
for (( k = 0; k < N; k++ )); do echo "   i$k terminou em ${ENDS[k]}s"; done
echo "   (o relógio começa quando as JVMs sobem; a consulta __op='r' no Databricks mede só a gravação)"
echo
# Para o CDC: o slot antigo aponta para um ponto do WAL de antes do snapshot, e reaproveitá-lo traria de novo
# mudanças que o snapshot já levou. Sem slot e sem offset, o no_data cria um slot novo na posição atual do WAL
# e segue dali. O que for escrito no Postgres entre o fim do snapshot e essa partida não é capturado:
# por isso ninguém escreve até o run.sh do passo 3 estar no ar.
echo "Para seguir com o CDC a partir daqui, sem refazer o snapshot:"
echo "  1) no psql:  SELECT pg_drop_replication_slot('zerobus_demo');   -- se existir"
echo "  2) rm debezium-server/dist/debezium-server/data/offsets.dat"
echo "  3) DEBEZIUM_SOURCE_SNAPSHOT_MODE=no_data ./debezium-server/run.sh"
