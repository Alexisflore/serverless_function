# GitHub Actions — Synchronisation horaire Shopify → Supabase

## Vue d'ensemble

Un workflow par store Shopify. Chacun installe les dépendances (Pipenv, Python 3.10) puis exécute `pipenv run python run_daily_sync.py` avec les variables d'environnement du store. Tous les stores écrivent dans la même base Supabase. Les lignes sont distinguées par `commercial_organisation`.

| Workflow | Store | Schedule (UTC) | Concurrency group | Auth Shopify |
|---|---|---|---|---|
| `process-daily-data.yml` | US | `'0 * * * *'` (minute 0) | `daily-sync-us` | Token statique `SHOPIFY_ACCESS_TOKEN` |
| `process-daily-data-jp.yml` | JP | `'20 * * * *'` (minute 20) | `daily-sync-jp` | Token statique `SHOPIFY_ACCESS_TOKEN_JP` |
| `process-daily-data-uk.yml` | UK | `'40 * * * *'` (minute 40) | `daily-sync-uk` | App Shopify CLI : client ID/secret, token obtenu au runtime (client credentials) |

Les minutes sont décalées pour que deux stores ne tournent jamais en même temps. Le `concurrency` group empêche deux exécutions du même store de se chevaucher.

## Secrets GitHub

`Settings` → `Secrets and variables` → `Actions`, ou `push-secrets-to-github.sh` (pousse les clés du `.env`).

| Secret | Utilisé par |
|---|---|
| `SHOPIFY_ACCESS_TOKEN`, `SHOPIFY_STORE_DOMAIN` | US |
| `SHOPIFY_ACCESS_TOKEN_JP`, `SHOPIFY_STORE_DOMAIN_JP` | JP |
| `SHOPIFY_STORE_DOMAIN_UK`, `SHOPIFY_CLIENT_ID_UK`, `SHOPIFY_CLIENT_SECRET_UK` | UK |
| `SHOPIFY_API_VERSION` | Tous |
| `SUPABASE_URL`, `SUPABASE_TOKEN` (exposé en `SUPABASE_SERVICE_ROLE_KEY`) | Tous |
| `SUPABASE_USER`, `SUPABASE_PASSWORD`, `SUPABASE_HOST`, `SUPABASE_PORT`, `SUPABASE_DB_NAME` | Tous |
| `CRON_SECRET` | Tous |

Le suffixe de store est sur le nom du secret GitHub. Le workflow le mappe vers le nom générique (`SHOPIFY_STORE_DOMAIN`, etc.) attendu par le code.

## Ajouter un store

1. Insérer la ligne dans `commercial_organization` (ex. `database_creation/add_uk_commercial_organization.sql`).
2. Créer l'app Shopify (`npm init @shopify/app@latest`), déclarer les scopes, installer l'app sur le store.
3. Ajouter les secrets `SHOPIFY_STORE_DOMAIN_<ORG>` et `SHOPIFY_CLIENT_ID_<ORG>` / `SHOPIFY_CLIENT_SECRET_<ORG>` (ou `SHOPIFY_ACCESS_TOKEN_<ORG>`).
4. Copier `process-daily-data-uk.yml`, changer les secrets, `COMMERCIAL_ORGANISATION`, le group de concurrency et la minute du cron.
5. Backfill de l'historique : `pipenv run python backfill_store_data.py --org <ORG>`, puis `check_store_backfill_status.py --org <ORG>`.

## Déclenchement manuel

`Actions` → choisir le workflow du store → `Run workflow` (ou `gh workflow run process-daily-data-uk.yml`).

## Points d'attention

- La synchro inventaire complète tourne quand l'heure du runner (UTC) vaut dimanche 02h. Elle tourne donc une fois par store, aux minutes décalées.
- Les queues webhook (`inventory_snapshot_queue`, `draft_orders_delete_queue`) sont filtrées par `shop` = `SHOPIFY_STORE_DOMAIN` du job.

## En cas d'échec

- Vérifier les logs dans l'onglet `Actions` (`gh run view <id> --log-failed`).
- Vérifier les secrets du store concerné.
- Store UK : si l'échange de token échoue (HTTP 4xx), vérifier que l'app est installée sur le store et que le client ID/secret correspondent.

## Désactiver un cron

`Actions` → `Workflows` → choisir le workflow → `...` → `Disable workflow`.
