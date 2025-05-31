# Adam Lippes Serverless Function

Ce projet contient une fonction serverless déployée sur Vercel qui récupère les commandes Shopify quotidiennement et les enregistre dans une base de données Supabase.

## Structure du projet

- `api/process_daily_data.py` : Fonction serverless principale qui traite les données quotidiennes
- `test_process_daily_data.py` : Script pour tester l'endpoint POST

## Comment tester la fonction serverless

### Prérequis

- Python 3.x
- Le module `requests` (`pip install requests`)
- Une variable d'environnement `CRON_SECRET` contenant la clé secrète

### Configuration de l'autorisation

La fonction serverless utilise une authentification par token Bearer. Pour que la requête soit autorisée, vous devez :

1. Définir la variable d'environnement `CRON_SECRET` sur votre machine :

   ```bash
   # Sur macOS/Linux
   export CRON_SECRET="votre_clé_secrète"
   
   # Sur Windows (PowerShell)
   $env:CRON_SECRET="votre_clé_secrète"
   ```

2. La même valeur doit être configurée dans les variables d'environnement de Vercel pour votre déploiement.

### Exécution du test

Pour tester la fonction, exécutez le script de test :

```bash
python test_process_daily_data.py
```

Ce script effectuera deux tests :
1. Un test avec l'autorisation correcte (devrait retourner un code 200)
2. Un test avec une autorisation invalide (devrait retourner un code 401)

### Format de l'en-tête d'autorisation

L'en-tête d'autorisation doit être au format :

```
Authorization: Bearer votre_clé_secrète
```

Où `votre_clé_secrète` est la valeur définie dans la variable d'environnement `CRON_SECRET`.

## Déploiement

La fonction est déployée sur Vercel à l'URL suivante :
https://adamlippes-o0jx78x73-alexisflores-projects.vercel.app/api/process_daily_data

Pour mettre à jour le déploiement, utilisez la commande :

```bash
vercel --prod
``` 