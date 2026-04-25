-- Colonne « Date » (format DD-MM-YYYY, dérivée de recorded_at en UTC) sur inventory_history
-- Implémentée comme colonne GÉNÉRÉE STORED : PostgreSQL garantit qu'elle est
-- toujours synchronisée avec recorded_at, sans trigger ni job applicatif.
-- Idempotent : peut être réexécuté sans risque.

-- 1) Nettoyage de l'ancienne implémentation à base de trigger (si présente)
DROP TRIGGER IF EXISTS trg_inventory_history_set_date ON inventory_history;
DROP FUNCTION IF EXISTS inventory_history_set_calendar_date();

-- 2) On retire la colonne existante (si elle existait en VARCHAR simple),
--    pour pouvoir la recréer en GENERATED. PostgreSQL n'autorise pas la
--    conversion d'une colonne classique en colonne générée via ALTER.
ALTER TABLE inventory_history
  DROP COLUMN IF EXISTS "Date";

-- 3) Recréation en colonne générée stockée
ALTER TABLE inventory_history
  ADD COLUMN "Date" CHARACTER VARYING(10)
  GENERATED ALWAYS AS (to_char((recorded_at AT TIME ZONE 'UTC'), 'DD-MM-YYYY')) STORED;

COMMENT ON COLUMN inventory_history."Date" IS
  'Date calendaire au format DD-MM-YYYY (UTC), dérivée automatiquement de recorded_at (colonne GÉNÉRÉE STORED).';
