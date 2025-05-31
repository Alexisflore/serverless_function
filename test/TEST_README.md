# Test de la fonction serverless process_daily_data

Ce dossier contient des scripts pour tester la fonction serverless `process_daily_data` qui récupère les commandes Shopify de la veille et les enregistre dans Supabase.

## Prérequis

- Python 3.9+
- Les dépendances du projet installées (`pip install -r requirements.txt`)
- Un fichier `.env` correctement configuré avec les variables d'environnement nécessaires

## Scripts disponibles

### 1. `test_process_daily_data.py`

Script Python qui envoie une requête POST à la fonction serverless avec l'authentification appropriée.

**Utilisation directe :**

```bash
python test_process_daily_data.py [--url URL] [--custom-secret SECRET]
```

**Options :**
- `--url` : URL de la fonction serverless (par défaut : http://localhost:3000/api/process_daily_data)
- `--custom-secret` : Secret personnalisé à utiliser au lieu de celui dans le fichier .env

### 2. `run_test.sh`

Script shell qui facilite l'exécution du test avec différentes configurations.

**Utilisation :**

```bash
./run_test.sh [options]
```

**Options :**
- `-h, --help` : Afficher l'aide
- `-l, --local` : Tester sur l'environnement local (par défaut)
- `-p, --production` : Tester sur l'environnement de production
- `-s, --secret <secret>` : Utiliser un secret personnalisé au lieu de celui dans .env

**Exemples :**
```bash
./run_test.sh                           # Test local avec le secret de .env
./run_test.sh -p                        # Test en production avec le secret de .env
./run_test.sh -s 'mon_secret'           # Test local avec un secret personnalisé
./run_test.sh -p -s 'mon_secret'        # Test en production avec un secret personnalisé
```

## Configuration

Avant d'exécuter les tests, assurez-vous que :

1. Le fichier `.env` contient la variable `CRON_SECRET` avec la valeur correcte
2. Si vous testez en production, modifiez l'URL dans `run_test.sh` pour pointer vers votre domaine Vercel
3. La fonction serverless est en cours d'exécution si vous testez en local

## Dépannage

- **Erreur 401 (Unauthorized)** : Vérifiez que le `CRON_SECRET` dans votre fichier `.env` correspond à celui configuré dans la fonction serverless
- **Erreur de connexion** : Assurez-vous que la fonction serverless est en cours d'exécution et accessible à l'URL spécifiée
- **Erreur 500** : Consultez les logs de la fonction serverless pour plus de détails sur l'erreur 