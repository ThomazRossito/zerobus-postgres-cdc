-- Demo Zerobus · catálogo retail_zerobus
-- Passo 3b: permissões do service principal. Rode DEPOIS de criar o service principal.
-- Troque as 4 ocorrências de <application-id> pelo Application ID dele (UUID, o mesmo client ID), mantendo as crases.
-- Regra do Zerobus: MODIFY e SELECT direto na tabela. ALL PRIVILEGES no schema não basta.

GRANT USE CATALOG ON CATALOG retail_zerobus TO `<application-id>`;
GRANT USE SCHEMA  ON SCHEMA  retail_zerobus.retail TO `<application-id>`;

GRANT SELECT, MODIFY ON TABLE retail_zerobus.retail.orders    TO `<application-id>`;
GRANT SELECT, MODIFY ON TABLE retail_zerobus.retail.inventory TO `<application-id>`;

-- Conferência: o service principal deve aparecer com MODIFY e SELECT nas duas tabelas
SHOW GRANTS ON TABLE retail_zerobus.retail.orders;
SHOW GRANTS ON TABLE retail_zerobus.retail.inventory;