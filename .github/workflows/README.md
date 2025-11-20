# GitHub Actions Cron Job Configuration

## Vue d'ensemble

Le workflow `process-daily-data.yml` déclenche automatiquement la synchronisation des données Shopify toutes les heures.

## Configuration requise

### Secrets à configurer dans GitHub

Pour que le workflow fonctionne, vous devez configurer les secrets suivants dans votre repository GitHub :

1. **Accéder aux secrets** :
   - Allez dans `Settings` → `Secrets and variables` → `Actions`
   - Cliquez sur `New repository secret`

2. **Secrets requis** :

   | Nom du secret | Description | Exemple |
   |---------------|-------------|---------|
   | `CRON_SECRET` | Le token d'authentification pour votre API Vercel | `your-secret-token-here` |
   | `VERCEL_URL` | L'URL complète de votre déploiement Vercel | `https://your-project.vercel.app` |

### Récupérer CRON_SECRET

Le `CRON_SECRET` doit correspondre à la variable d'environnement définie dans votre projet Vercel :

1. Allez dans votre projet Vercel
2. `Settings` → `Environment Variables`
3. Trouvez la variable `CRON_SECRET`
4. Copiez sa valeur

### Récupérer VERCEL_URL

1. Allez dans votre projet Vercel
2. Dans le Dashboard, copiez l'URL de production (ex: `https://cron-functions.vercel.app`)
3. ⚠️ **Important** : N'incluez PAS de slash final `/` dans l'URL

## Schedule du Cron

```yaml
'0 * * * *'  # Toutes les heures à la minute 0
```

### Autres exemples de schedule :

```yaml
'*/30 * * * *'   # Toutes les 30 minutes
'0 */2 * * *'    # Toutes les 2 heures
'0 9 * * *'      # Tous les jours à 9h00 UTC
'0 */4 * * *'    # Toutes les 4 heures
```

## Déclenchement manuel

Vous pouvez également déclencher le workflow manuellement :

1. Allez dans l'onglet `Actions` de votre repository
2. Sélectionnez le workflow "Process Daily Data - Hourly Sync"
3. Cliquez sur `Run workflow`

## Surveillance

### Vérifier l'exécution

1. Allez dans l'onglet `Actions` de votre repository
2. Vous verrez l'historique de toutes les exécutions
3. Cliquez sur une exécution pour voir les logs détaillés

### En cas d'échec

Si le workflow échoue :
- Vérifiez que les secrets sont correctement configurés
- Vérifiez les logs dans l'onglet Actions
- Vérifiez que votre fonction Vercel est bien déployée et accessible
- Vérifiez les logs de votre fonction Vercel

## Désactiver le cron

Pour désactiver temporairement le cron sans supprimer le workflow :

1. Commentez la section `schedule` dans le fichier `process-daily-data.yml`
2. Commitez et poussez les changements

Ou directement depuis l'interface GitHub :
1. `Actions` → `Workflows` → "Process Daily Data - Hourly Sync"
2. Cliquez sur les trois points `...`
3. Sélectionnez `Disable workflow`

## Notifications

Par défaut, GitHub vous envoie un email si un workflow échoue. Vous pouvez configurer ces notifications dans :
- `Settings` → `Notifications` → `Actions`

