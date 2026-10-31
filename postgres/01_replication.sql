-- Zerobus + Debezium Server · passo 1 · habilitar replicação lógica no PostgreSQL local
-- Rode como superusuário (ex.: psql -U postgres -d postgres -f postgres/01_replication.sql)
--
-- wal_level=logical exige REINICIAR o PostgreSQL depois deste script.
-- Homebrew:  brew services restart postgresql@16   (ajuste a versão)
-- Postgres.app: Stop / Start no app.

ALTER SYSTEM SET wal_level = 'logical';
ALTER SYSTEM SET max_replication_slots = 4;
ALTER SYSTEM SET max_wal_senders = 4;

-- Usuário que o Debezium usa para ler o WAL. REPLICATION é obrigatório para o slot.
-- Troque a senha e mantenha a mesma no .env (PG_PASSWORD).
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dbz') THEN
    CREATE ROLE dbz WITH LOGIN REPLICATION PASSWORD 'dbz_local_2026';
  END IF;
END $$;

-- Conferência (depois do restart): deve mostrar 'logical'
-- SHOW wal_level;

SELECT pg_postmaster_start_time(),
       current_setting('wal_level')             AS wal_level,
       current_setting('max_replication_slots') AS slots,
       current_setting('max_wal_senders')       AS senders;

SELECT rolname, rolreplication, rolcanlogin FROM pg_roles WHERE rolname = 'dbz';