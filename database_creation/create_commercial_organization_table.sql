-- =================================================================================
-- MIGRATION : Création de la table dimension commercial_organization
-- =================================================================================
-- Objectif : Créer une table de référence (dimension) pour les organisations
--            commerciales (pays / stores) afin que toutes les tables de faits
--            puissent y être rattachées via une clé étrangère.
--
-- - commercial_organization_code  : code court (ex: 'US', 'JP')
-- - commercial_organization_label : libellé lisible (ex: 'United-States', 'Japon')
--
-- La colonne `commercial_organisation` (VARCHAR(10) NOT NULL) déjà présente sur
-- toutes les tables est conservée et devient la clé étrangère vers cette
-- nouvelle dimension.
-- =================================================================================

BEGIN;

-- ============================================
-- ÉTAPE 1 : Création de la table dimension
-- ============================================

CREATE TABLE IF NOT EXISTS commercial_organization (
    commercial_organization_code  VARCHAR(10)  PRIMARY KEY,
    commercial_organization_label VARCHAR(100) NOT NULL,
    is_active                     BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at                    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at                    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE  commercial_organization                                 IS 'Table de dimension des organisations commerciales (pays / stores) — référencée par toutes les tables de faits via la colonne commercial_organisation';
COMMENT ON COLUMN commercial_organization.commercial_organization_code   IS 'Code court de l''organisation commerciale (ex: US, JP, FR, UK) — clé primaire';
COMMENT ON COLUMN commercial_organization.commercial_organization_label  IS 'Libellé lisible de l''organisation commerciale (ex: United-States, Japon)';
COMMENT ON COLUMN commercial_organization.is_active                      IS 'Indique si l''organisation est actuellement active dans le périmètre';

-- ============================================
-- ÉTAPE 2 : Population initiale (US, JP)
-- ============================================

INSERT INTO commercial_organization (commercial_organization_code, commercial_organization_label)
VALUES
    ('US', 'United-States'),
    ('JP', 'Japon')
ON CONFLICT (commercial_organization_code) DO UPDATE
    SET commercial_organization_label = EXCLUDED.commercial_organization_label,
        updated_at = NOW();

-- ============================================
-- ÉTAPE 3 : Trigger de mise à jour de updated_at
-- ============================================

CREATE OR REPLACE FUNCTION set_commercial_organization_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at := NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_commercial_organization_updated_at ON commercial_organization;
CREATE TRIGGER trg_commercial_organization_updated_at
    BEFORE UPDATE ON commercial_organization
    FOR EACH ROW
    EXECUTE FUNCTION set_commercial_organization_updated_at();

-- ============================================
-- ÉTAPE 4 : Garde-fou — vérifier qu'aucune valeur orpheline n'existe
-- ============================================
-- Si une valeur de `commercial_organisation` n'a pas de correspondance dans
-- la dimension, la migration est interrompue afin de ne pas perdre de données.

DO $$
DECLARE
    orphan_count INTEGER;
    tbl TEXT;
BEGIN
    FOR tbl IN
        SELECT unnest(ARRAY[
            'orders', 'orders_details', 'transaction', 'customers',
            'draft_order', 'payout', 'payout_transaction',
            'inventory', 'inventory_history', 'locations', 'products'
        ])
    LOOP
        EXECUTE format(
            'SELECT COUNT(*) FROM %I t
             WHERE t.commercial_organisation IS NOT NULL
               AND NOT EXISTS (
                   SELECT 1 FROM commercial_organization d
                   WHERE d.commercial_organization_code = t.commercial_organisation
               )', tbl
        ) INTO orphan_count;

        IF orphan_count > 0 THEN
            RAISE EXCEPTION
                'Migration impossible : % ligne(s) orpheline(s) dans %.commercial_organisation. Ajoutez les codes manquants à commercial_organization avant de relancer.',
                orphan_count, tbl;
        END IF;
    END LOOP;
END $$;

-- ============================================
-- ÉTAPE 5 : Création des clés étrangères
-- ============================================
-- Chaque table de faits référence la dimension via son code.

DO $$
DECLARE
    tbl TEXT;
    fk_name TEXT;
BEGIN
    FOR tbl IN
        SELECT unnest(ARRAY[
            'orders', 'orders_details', 'transaction', 'customers',
            'draft_order', 'payout', 'payout_transaction',
            'inventory', 'inventory_history', 'locations', 'products'
        ])
    LOOP
        fk_name := 'fk_' || tbl || '_commercial_organization';

        IF NOT EXISTS (
            SELECT 1 FROM information_schema.table_constraints
            WHERE table_schema = 'public'
              AND table_name   = tbl
              AND constraint_name = fk_name
        ) THEN
            EXECUTE format(
                'ALTER TABLE %I
                 ADD CONSTRAINT %I
                 FOREIGN KEY (commercial_organisation)
                 REFERENCES commercial_organization (commercial_organization_code)
                 ON UPDATE CASCADE
                 ON DELETE RESTRICT',
                tbl, fk_name
            );
            RAISE NOTICE 'FK ajoutée : %', fk_name;
        ELSE
            RAISE NOTICE 'FK déjà existante (skip) : %', fk_name;
        END IF;
    END LOOP;
END $$;

-- ============================================
-- ÉTAPE 6 : Vérification finale
-- ============================================

DO $$
DECLARE
    fk_count       INTEGER;
    dimension_rows INTEGER;
BEGIN
    SELECT COUNT(*) INTO dimension_rows FROM commercial_organization;

    SELECT COUNT(*) INTO fk_count
    FROM information_schema.table_constraints
    WHERE table_schema = 'public'
      AND constraint_type = 'FOREIGN KEY'
      AND constraint_name LIKE 'fk_%_commercial_organization';

    RAISE NOTICE '========================================================================';
    RAISE NOTICE 'commercial_organization : % ligne(s) en dimension', dimension_rows;
    RAISE NOTICE 'Clés étrangères créées  : % / 11 attendues', fk_count;
    RAISE NOTICE '========================================================================';
END $$;

COMMIT;
