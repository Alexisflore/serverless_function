-- Migration: Ajouter orders_details_id à la table transaction
-- Date: 2025-06-13
-- Description: Permet de lier les transactions aux orders_details spécifiques

-- Étape 1: Ajouter la colonne orders_details_id
ALTER TABLE transaction 
ADD COLUMN orders_details_id BIGINT;

-- Étape 2: Populer la colonne pour les transactions avec product_id
-- (Uniquement pour les transactions liées à des articles spécifiques)
UPDATE transaction t
SET orders_details_id = od._id_order_detail
FROM orders_details od
WHERE t.order_id = od._id_order
  AND t.product_id = od._id_product
  AND t.variant_id = od.variant_id
  AND t.product_id IS NOT NULL
  AND t.variant_id IS NOT NULL;

-- Étape 3: Ajouter la contrainte de clé étrangère
ALTER TABLE transaction 
ADD CONSTRAINT fk_transaction_orders_details 
FOREIGN KEY (orders_details_id) 
REFERENCES orders_details(_id_order_detail);

-- Étape 4: Créer un index pour améliorer les performances des jointures
CREATE INDEX idx_transaction_orders_details_id 
ON transaction(orders_details_id);

-- Étape 5: Créer un index composite pour les requêtes fréquentes
CREATE INDEX idx_transaction_order_product_variant 
ON transaction(order_id, product_id, variant_id) 
WHERE product_id IS NOT NULL AND variant_id IS NOT NULL;

-- Vérification: Compter les liaisons réussies
SELECT 
    'Transactions avec orders_details_id' as type,
    COUNT(*) as count
FROM transaction 
WHERE orders_details_id IS NOT NULL
UNION ALL
SELECT 
    'Transactions sans orders_details_id' as type,
    COUNT(*) as count
FROM transaction 
WHERE orders_details_id IS NULL
UNION ALL
SELECT 
    'Total transactions' as type,
    COUNT(*) as count
FROM transaction; 