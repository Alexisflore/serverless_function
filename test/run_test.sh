#!/bin/bash
# run_test.sh - Script pour faciliter l'exécution du test de la fonction serverless

# Couleurs pour les messages
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Afficher l'aide
show_help() {
    echo -e "${YELLOW}Usage:${NC} ./run_test.sh [options]"
    echo ""
    echo "Options:"
    echo "  -h, --help                Afficher cette aide"
    echo "  -l, --local               Tester sur l'environnement local (par défaut)"
    echo "  -p, --production          Tester sur l'environnement de production"
    echo "  -s, --secret <secret>     Utiliser un secret personnalisé au lieu de celui dans .env"
    echo "  -d, --debug               Activer le mode debug pour voir plus de logs"
    echo "  -t, --test-db             Tester directement la fonction check_and_update_order"
    echo "  -r, --real-data           Utiliser de vraies données du jour pour le test"
    echo ""
    echo "Exemples:"
    echo "  ./run_test.sh                           # Test local avec le secret de .env"
    echo "  ./run_test.sh -p                        # Test en production avec le secret de .env"
    echo "  ./run_test.sh -s 'mon_secret'           # Test local avec un secret personnalisé"
    echo "  ./run_test.sh -t                        # Test direct de la fonction check_and_update_order"
    echo "  ./run_test.sh -t -d                     # Test direct avec mode debug activé"
    echo "  ./run_test.sh -t -r                     # Test direct avec de vraies données"
    echo "  ./run_test.sh -t -r -d                  # Test direct avec de vraies données et mode debug"
}

# Valeurs par défaut
URL_LOCAL="http://localhost:3000/api/process_daily_data.py"
URL_PROD="https://adamlippes.vercel.app/api/process_daily_data.py"
USE_PROD=false
CUSTOM_SECRET=""
DEBUG_MODE=false
TEST_DB=false
REAL_DATA=false

# Analyser les arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_help
            exit 0
            ;;
        -l|--local)
            USE_PROD=false
            shift
            ;;
        -p|--production)
            USE_PROD=true
            shift
            ;;
        -s|--secret)
            CUSTOM_SECRET="$2"
            shift
            shift
            ;;
        -d|--debug)
            DEBUG_MODE=true
            shift
            ;;
        -t|--test-db)
            TEST_DB=true
            shift
            ;;
        -r|--real-data)
            REAL_DATA=true
            shift
            ;;
        *)
            echo -e "${RED}Option non reconnue: $1${NC}"
            show_help
            exit 1
            ;;
    esac
done

# Déterminer l'URL à utiliser
if [ "$USE_PROD" = true ]; then
    URL=$URL_PROD
    ENV_TYPE="production"
else
    URL=$URL_LOCAL
    ENV_TYPE="local"
fi

# Construire la commande de base
COMMAND="python test/test_process_daily_data.py"

# Ajouter les options à la commande
if [ "$TEST_DB" = true ]; then
    COMMAND="$COMMAND --test-db"
    echo -e "${GREEN}=== Test direct de la fonction check_and_update_order ===${NC}"
else
    COMMAND="$COMMAND --url \"$URL\""
    echo -e "${GREEN}=== Test de la fonction serverless process_daily_data ===${NC}"
    echo -e "Environnement: ${YELLOW}$ENV_TYPE${NC}"
    echo -e "URL: ${YELLOW}$URL${NC}"
fi

if [ "$DEBUG_MODE" = true ]; then
    COMMAND="$COMMAND --debug"
    echo -e "Mode debug: ${YELLOW}Activé${NC}"
else
    echo -e "Mode debug: ${YELLOW}Désactivé${NC}"
fi

if [ "$REAL_DATA" = true ]; then
    COMMAND="$COMMAND --real-data"
    echo -e "Données: ${YELLOW}Réelles${NC}"
else
    echo -e "Données: ${YELLOW}Test${NC}"
fi

if [ -n "$CUSTOM_SECRET" ]; then
    COMMAND="$COMMAND --custom-secret \"$CUSTOM_SECRET\""
    echo -e "Secret: ${YELLOW}Personnalisé${NC}"
else
    echo -e "Secret: ${YELLOW}Depuis .env${NC}"
fi

echo -e "${GREEN}=== Exécution du test ===${NC}"
echo -e "Commande: ${YELLOW}$COMMAND${NC}"
echo ""

# Exécuter la commande
eval $COMMAND 