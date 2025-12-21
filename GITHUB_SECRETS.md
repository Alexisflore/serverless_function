# Secrets GitHub requis pour le workflow

Ce document liste tous les secrets GitHub nécessaires pour le workflow `process-daily-data.yml`.

## Comment ajouter les secrets

### Option 1 : Utiliser le script automatique
```bash
./push-secrets-to-github.sh
```

### Option 2 : Manuellement via GitHub CLI
```bash
# Pour chaque secret:
echo "valeur_du_secret" | gh secret set NOM_DU_SECRET
```

### Option 3 : Via l'interface GitHub
1. Aller sur https://github.com/Alexisflore/serverless_function/settings/secrets/actions
2. Cliquer sur "New repository secret"
3. Ajouter chaque secret individuellement

## Liste des secrets requis

### 🛒 Shopify API
- `SHOPIFY_ACCESS_TOKEN` - Token d'accès à l'API Shopify
- `SHOPIFY_STORE_DOMAIN` - Domaine du store (ex: your-store.myshopify.com)
- `SHOPIFY_API_VERSION` - Version de l'API (ex: 2025-01)
- `SHOPIFY_API_KEY` - Clé API Shopify
- `SHOPIFY_API_SECRET` - Secret API Shopify
- `SHOPIFY_SHOP_NAME` - Nom du shop

### 🗄️ Supabase
- `SUPABASE_URL` - URL du projet Supabase (ex: https://xxx.supabase.co)
- `SUPABASE_KEY` - Clé anon publique Supabase
- `SUPABASE_SERVICE_ROLE_KEY` - Clé service role Supabase (privilèges admin)

### 💾 Database (Supabase Postgres)
- `DATABASE_URL` - URL complète de connexion PostgreSQL
- `SUPABASE_USER` - Utilisateur DB (généralement: postgres)
- `SUPABASE_PASSWORD` - Mot de passe DB
- `SUPABASE_HOST` - Host DB (ex: db.xxx.supabase.co)
- `SUPABASE_PORT` - Port DB (généralement: 5432)
- `SUPABASE_DB_NAME` - Nom de la DB (généralement: postgres)

### 🔒 Security
- `CRON_SECRET` - Secret pour sécuriser les appels cron

## Vérification

Pour vérifier que tous les secrets sont bien configurés:
```bash
gh secret list
```

## Total des secrets
**15 secrets** doivent être configurés pour que le workflow fonctionne correctement.

