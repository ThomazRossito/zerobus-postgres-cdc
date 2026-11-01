#!/usr/bin/env bash
# Baixa o Debezium Server (versão fixada), instala o application.properties e sobe o processo.
# Uso:  ./debezium-server/run.sh           (a partir da raiz do projeto)
#       ENV_FILE=.env.free ./debezium-server/run.sh   (outro workspace, com slot e offsets próprios no .env.free)
set -euo pipefail

DBZ_VERSION="${DBZ_VERSION:-3.7.0.Final}"
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
DIST_DIR="$HERE/dist"
TARBALL="debezium-server-dist-${DBZ_VERSION}.tar.gz"
URL="https://repo1.maven.org/maven2/io/debezium/debezium-server-dist/${DBZ_VERSION}/${TARBALL}"

# 1) variáveis
ENV_FILE="${ENV_FILE:-$ROOT/.env}"   # outro workspace: ENV_FILE=.env.free ./debezium-server/run.sh
[[ "$ENV_FILE" = /* ]] || ENV_FILE="$ROOT/$ENV_FILE"
if [[ -f "$ENV_FILE" ]]; then
  set -a; # exporta tudo que o .env definir
  # shellcheck disable=SC1091
  source "$ENV_FILE"
  set +a
else
  echo "faltou $ENV_FILE (copie de .env.example)"; exit 1
fi
for v in ZEROBUS_SERVER_ENDPOINT DATABRICKS_WORKSPACE_URL DATABRICKS_CLIENT_ID DATABRICKS_CLIENT_SECRET UC_CATALOG PG_HOST PG_PORT PG_USER PG_PASSWORD PG_DBNAME; do
  [[ -n "${!v:-}" ]] || { echo "variável $v vazia em $ENV_FILE"; exit 1; }
done

# 2) Java 21+ (Debezium 3.x compila para Java 21)
#    Procura um Java 21+ no PATH, no openjdk@21 do Homebrew (keg-only, fora do PATH por padrão) e no
#    java_home do macOS. O run.sh do Debezium usa $JAVA_HOME/bin/java, então o JAVA_HOME passa a ser
#    o desse Java, só para este processo: o Java padrão do Mac (ex.: 17) continua o mesmo.
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
export PATH="$JH/bin:$PATH"
echo "usando Java $(java_major "$JH/bin/java"): $JH"

# 3) distribuição
mkdir -p "$DIST_DIR"
if [[ ! -x "$DIST_DIR/debezium-server/run.sh" ]]; then
  echo "baixando $URL"
  curl -fL --retry 3 -o "$DIST_DIR/$TARBALL" "$URL"
  tar -xzf "$DIST_DIR/$TARBALL" -C "$DIST_DIR"
fi

# 3b) SMT ExtractNewRecordState (o "unwrap"): no Debezium 3.7 ele fica no jar debezium-connect-plugins.
#     Se a distribuição não trouxer esse jar em lib/, o Debezium cai com
#     ClassNotFoundException: io.debezium.transforms.ExtractNewRecordState
LIB="$DIST_DIR/debezium-server/lib"
if ! ls "$LIB"/debezium-connect-plugins-*.jar >/dev/null 2>&1; then
  FOUND=$(find "$DIST_DIR/debezium-server" -name "debezium-connect-plugins-*.jar" 2>/dev/null | head -1)
  if [[ -n "$FOUND" ]]; then
    cp "$FOUND" "$LIB/"
  else
    PJAR="debezium-connect-plugins-${DBZ_VERSION}.jar"
    echo "baixando $PJAR (traz o SMT ExtractNewRecordState)"
    curl -fL --retry 3 -o "$LIB/$PJAR.part" \
      "https://repo1.maven.org/maven2/io/debezium/debezium-connect-plugins/${DBZ_VERSION}/${PJAR}"
    mv "$LIB/$PJAR.part" "$LIB/$PJAR"   # só vira .jar depois do download completo
  fi
fi
cd "$DIST_DIR/debezium-server"
# o Debezium Server lê config/application.properties (pasta config, não conf)
mkdir -p config data
cp "$HERE/application.properties" config/application.properties

# 4) sobe. Os ${VAR} do application.properties são resolvidos pelo Quarkus a partir do ambiente.
#    A distribuição traz a extensão de config do Kubernetes do Quarkus, que monta um cliente Kubernetes
#    na partida e lê o ~/.kube/config. Um kubeconfig quebrado (ex.: minikube apagado) derruba o
#    Debezium antes de começar. Aqui não tem Kubernetes: o cliente não lê kubeconfig nenhum.
export KUBERNETES_AUTH_TRYKUBECONFIG=false
export KUBECONFIG=/dev/null
echo "Debezium Server $DBZ_VERSION -> Zerobus ($ZEROBUS_SERVER_ENDPOINT), catálogo $UC_CATALOG, variáveis de ${ENV_FILE#"$ROOT/"}"
exec ./run.sh