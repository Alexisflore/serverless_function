#!/bin/bash

# Script pour envoyer les secrets du .env vers GitHub
# Usage: ./push-secrets-to-github.sh

set -e

# Couleurs pour les messages
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Vérifier que gh est installé
if ! command -v gh &> /dev/null; then
    echo -e "${RED}❌ GitHub CLI (gh) n'est pas installé${NC}"
    echo "Installez-le avec: brew install gh"
    exit 1
fi

# Vérifier que le fichier .env existe
if [ ! -f ".env" ]; then
    echo -e "${RED}❌ Fichier .env introuvable${NC}"
    exit 1
fi

# Vérifier l'authentification GitHub
if ! gh auth status &> /dev/null; then
    echo -e "${RED}❌ Vous n'êtes pas authentifié avec GitHub CLI${NC}"
    echo "Authentifiez-vous avec: gh auth login"
    exit 1
fi

# Obtenir le repository actuel
repo=$(gh repo view --json nameWithOwner -q .nameWithOwner 2>/dev/null)
if [ -z "$repo" ]; then
    echo -e "${RED}❌ Impossible de déterminer le repository GitHub${NC}"
    echo "Assurez-vous d'être dans un dossier Git avec un remote GitHub"
    exit 1
fi

branch=$(git branch --show-current 2>/dev/null || echo "unknown")

echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${YELLOW}📋 Informations:${NC}"
echo -e "   Repository: ${GREEN}$repo${NC}"
echo -e "   Branche: ${GREEN}$branch${NC}"
echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
read -p "Voulez-vous continuer et envoyer les secrets vers ce repository? (y/N) " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${YELLOW}❌ Opération annulée${NC}"
    exit 0
fi

echo ""
echo -e "${GREEN}🔐 Envoi des secrets depuis .env vers GitHub...${NC}"
echo ""

# Compter le nombre de secrets
total_secrets=0
success_count=0
error_count=0

# Lire le fichier .env et envoyer chaque variable comme secret
while IFS= read -r line || [ -n "$line" ]; do
    # Ignorer les lignes vides et les commentaires
    if [[ -z "$line" ]] || [[ "$line" =~ ^[[:space:]]*# ]]; then
        continue
    fi
    
    # Extraire le nom et la valeur de la variable
    if [[ "$line" =~ ^[[:space:]]*([A-Za-z_][A-Za-z0-9_]*)=(.*)$ ]]; then
        var_name="${BASH_REMATCH[1]}"
        var_value="${BASH_REMATCH[2]}"
        
        # Enlever les guillemets si présents
        var_value=$(echo "$var_value" | sed -e 's/^"//' -e 's/"$//' -e "s/^'//" -e "s/'$//")
        
        ((total_secrets++))
        
        echo -n "📤 Envoi de $var_name... "
        
        # Envoyer le secret à GitHub
        if echo "$var_value" | gh secret set "$var_name" 2>/dev/null; then
            echo -e "${GREEN}✓${NC}"
            ((success_count++))
        else
            echo -e "${RED}✗${NC}"
            ((error_count++))
        fi
    fi
done < .env

echo ""
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "📊 Résumé:"
echo -e "   Total: $total_secrets secrets"
echo -e "   ${GREEN}Succès: $success_count${NC}"
if [ $error_count -gt 0 ]; then
    echo -e "   ${RED}Erreurs: $error_count${NC}"
fi
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

if [ $error_count -eq 0 ]; then
    echo -e "${GREEN}✅ Tous les secrets ont été envoyés avec succès!${NC}"
else
    echo -e "${YELLOW}⚠️  Certains secrets n'ont pas pu être envoyés${NC}"
fi

