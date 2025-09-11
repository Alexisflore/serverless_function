from supabase import create_client
import os
import psycopg2
import logging
import decimal
import datetime

# Configuration du logging pour Vercel
from .logging_config import get_logger
logger = get_logger('database')

def get_supabase_client():
    """
    Get the Supabase client
    """
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    return create_client(supabase_url, supabase_key)

def check_and_update_order(cur, order, column_types):
    """
    Vérifie si une commande existe déjà dans la base de données.
    Si elle existe, vérifie si des colonnes ont changé et met à jour si nécessaire.
    Retourne True si une insertion ou mise à jour a été effectuée, False sinon.
    """
    logger.info("Début de check_and_update_order")
    logger.info(f"Commande à vérifier: {order}")
    
    # Vérifier si l'ID existe (vérifier 'Id' et 'id')
    id_key = None
    for key in ['Id', 'id']:
        if key in order:
            id_key = key
            break
            
    if id_key is None:
        logger.warning("Pas d'ID dans la commande, insertion directe")
        # Pas d'ID, on ne peut pas vérifier l'existence, on procède à l'insertion
        return True
    
    order_id = order[id_key]
    logger.info(f"ID de la commande: {order_id}")
    
    # Convertir l'ID au format de la base de données
    db_id_column = id_key  # Utiliser le même format que dans les données
    
    # Vérifier si la commande existe déjà
    check_query = f"SELECT * FROM orders WHERE {db_id_column} = %s"
    logger.info(f"Requête de vérification: {check_query} avec ID: {order_id}")
    
    try:
        cur.execute(check_query, (order_id,))
        existing_order = cur.fetchone()
        
        if not existing_order:
            logger.info(f"La commande avec ID {order_id} n'existe pas, insertion nécessaire")
            # La commande n'existe pas, on procède à l'insertion
            return True
        
        logger.info(f"Commande existante trouvée avec ID {order_id}")
        
        # Récupérer les noms des colonnes
        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'orders'")
        column_names = [row[0] for row in cur.fetchall()]
        logger.debug(f"Noms des colonnes: {column_names}")
        
        # Créer un dictionnaire pour l'ordre existant
        existing_order_dict = dict(zip(column_names, existing_order))
        logger.debug(f"Commande existante: {existing_order_dict}")
        
        # Préparer les colonnes et les valeurs pour la mise à jour
        update_columns = []
        update_values = []
        
        for key, new_value in order.items():
            # Convertir les clés au format de la base de données (préserver la casse)
            column_name = key.lower().replace(" ", "_")
            
            # Vérifier si la colonne existe dans la table
            if column_name not in column_types:
                logger.warning(f"Colonne {column_name} non trouvée dans les types de colonnes")
                continue
            
            # Récupérer le type de la colonne
            column_info = column_types[column_name]
            column_type = column_info['type'].lower()
            max_length = column_info['max_length']
            
            # Valeur avant traitement
            logger.debug(f"Colonne {column_name}: nouvelle valeur avant traitement = {new_value}, type = {type(new_value)}")
            
            # Traiter la valeur en fonction du type de colonne
            if new_value is None or new_value == "":
                if column_type != "text" and column_type != "character varying":
                    new_value = None
            elif column_type in ["numeric", "decimal", "real", "double precision"]:
                try:
                    new_value = float(new_value) if new_value is not None else None
                except (ValueError, TypeError):
                    logger.warning(f"Impossible de convertir {new_value} en float pour la colonne {column_name}")
                    new_value = None
            elif column_type in ["integer", "bigint", "smallint"]:
                try:
                    new_value = int(new_value) if new_value is not None else None
                except (ValueError, TypeError):
                    logger.warning(f"Impossible de convertir {new_value} en int pour la colonne {column_name}")
                    new_value = None
            elif column_type in ["timestamp", "timestamp without time zone", "timestamp with time zone", "date"]:
                # Pas de conversion pour les timestamps, on les laisse en chaîne pour la mise à jour
                # Mais on peut essayer de normaliser le format pour éviter des mises à jour inutiles
                if isinstance(new_value, str):
                    try:
                        # Normaliser le format de date si possible
                        if 'T' in new_value:
                            # Format ISO avec T
                            date_part = new_value.split('T')[0]
                            time_part = new_value.split('T')[1].split('-')[0].split('+')[0] if 'T' in new_value else ""
                            if time_part and ':' in time_part:
                                # Si on a une partie heure, la garder
                                new_value = f"{date_part} {time_part}"
                            else:
                                # Sinon, juste la date
                                new_value = date_part
                    except (ValueError, IndexError) as e:
                        logger.warning(f"Erreur lors de la normalisation de la date {new_value}: {e}")
            elif column_type == "boolean":
                if isinstance(new_value, str):
                    new_value = new_value.lower() in ["true", "t", "yes", "y", "1"]
            elif column_type == "character varying" and max_length is not None:
                if isinstance(new_value, str) and len(new_value) > max_length:
                    logger.warning(f"Valeur tronquée pour {column_name}: {len(new_value)} > {max_length}")
                    new_value = new_value[:max_length]
            
            # Valeur après traitement
            logger.debug(f"Colonne {column_name}: nouvelle valeur après traitement = {new_value}, type = {type(new_value)}")
            
            # Comparer avec la valeur existante
            existing_value = existing_order_dict.get(column_name)
            logger.debug(f"Colonne {column_name}: valeur existante = {existing_value}, type = {type(existing_value)}")
            
            # Fonction pour comparer les valeurs en tenant compte des types
            def values_are_equal(val1, val2):
                # Si les deux sont None, ils sont égaux
                if val1 is None and val2 is None:
                    return True
                
                # Si l'un est None et l'autre non, ils sont différents
                if val1 is None or val2 is None:
                    # Exception pour les chaînes vides et None qui sont considérés équivalents
                    if (val1 == "" and val2 is None) or (val1 is None and val2 == ""):
                        return True
                    return False
                
                # Pour les nombres, convertir en float pour la comparaison
                if isinstance(val1, (int, float, decimal.Decimal)) and isinstance(val2, (int, float, decimal.Decimal)):
                    # Utiliser une comparaison avec une petite marge d'erreur pour les nombres à virgule flottante
                    return abs(float(val1) - float(val2)) < 0.000001
                
                # Pour les timestamps PostgreSQL (qui sont des objets datetime en Python)
                if isinstance(val1, datetime.datetime):
                    # Si val2 est une chaîne, la convertir en datetime pour comparaison
                    if isinstance(val2, str):
                        # Supprimer le fuseau horaire de la chaîne
                        clean_val2 = val2
                        if 'T' in clean_val2:
                            parts = clean_val2.split('T')
                            date_part = parts[0]
                            if len(parts) > 1:
                                time_part = parts[1]
                                # Supprimer le fuseau horaire
                                if '+' in time_part:
                                    time_part = time_part.split('+')[0]
                                elif '-' in time_part and time_part.count('-') == 1:
                                    time_part = time_part.split('-')[0]
                                clean_val2 = f"{date_part} {time_part}"
                        
                        # Remplacer 'T' par un espace
                        clean_val2 = clean_val2.replace('T', ' ').strip()
                        
                        # Extraire seulement la date et l'heure jusqu'à la minute
                        # Format: YYYY-MM-DD HH:MM
                        val1_str = val1.strftime("%Y-%m-%d %H:%M")
                        
                        # Tronquer val2 à la même longueur
                        if len(clean_val2) >= 16:
                            val2_str = clean_val2[:16]
                        else:
                            # Si val2 est juste une date sans heure
                            if len(clean_val2) == 10:  # YYYY-MM-DD
                                val1_str = val1.strftime("%Y-%m-%d")
                                val2_str = clean_val2
                            else:
                                val2_str = clean_val2
                        
                        logger.debug(f"Comparaison de timestamps: '{val1_str}' vs '{val2_str}'")
                        return val1_str == val2_str
                
                # Pour les dates/timestamps, comparer seulement les dates sans le fuseau horaire
                if isinstance(val1, datetime.datetime) and isinstance(val2, str):
                    # Extraire la date de la chaîne (ignorer le fuseau horaire)
                    try:
                        # Essayer d'extraire la date sans le fuseau horaire
                        date_str = val2.split('T')[0] if 'T' in val2 else val2
                        time_str = val2.split('T')[1].split('-')[0] if 'T' in val2 and '-' in val2.split('T')[1] else ""
                        if not time_str and 'T' in val2:
                            time_str = val2.split('T')[1].split('+')[0] if '+' in val2.split('T')[1] else val2.split('T')[1]
                        
                        datetime_str = f"{date_str} {time_str}".strip()
                        
                        # Gérer différents formats de date
                        try:
                            if ':' in time_str:
                                parsed_date = datetime.datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
                            else:
                                parsed_date = datetime.datetime.strptime(date_str, "%Y-%m-%d")
                            
                            # Comparer seulement la date et l'heure sans les secondes pour éviter les problèmes de fuseaux horaires
                            return (val1.year == parsed_date.year and 
                                    val1.month == parsed_date.month and 
                                    val1.day == parsed_date.day and 
                                    val1.hour == parsed_date.hour and 
                                    val1.minute == parsed_date.minute)
                        except ValueError:
                            # Si le format ne correspond pas, essayer d'autres formats
                            logger.warning(f"Format de date non reconnu: {datetime_str}")
                            return False
                    except (ValueError, IndexError) as e:
                        logger.warning(f"Erreur lors de la comparaison de dates: {e} - {val1} vs {val2}")
                        return False
                
                # Pour les autres types, comparaison directe
                return val1 == val2
            
            # Si la valeur a changé, l'ajouter à la liste des colonnes à mettre à jour
            if not values_are_equal(new_value, existing_value):
                logger.info(f"Différence détectée pour {column_name}: {existing_value} -> {new_value}")
                update_columns.append(column_name)
                update_values.append(new_value)
        
        # Si des colonnes ont changé, effectuer la mise à jour
        if update_columns:
            placeholders = [f"{col} = %s" for col in update_columns]
            update_query = f"UPDATE orders SET {', '.join(placeholders)} WHERE {db_id_column} = %s"
            update_values.append(order_id)  # Ajouter l'ID à la fin pour la clause WHERE
            
            logger.info(f"Mise à jour nécessaire pour ID {order_id}. Requête: {update_query}")
            logger.info(f"Valeurs pour la mise à jour: {update_values}")
            
            cur.execute(update_query, update_values)
            logger.info(f"Mise à jour réussie pour ID {order_id}")
            return True
        
        # Aucune mise à jour nécessaire
        logger.info(f"Aucune mise à jour nécessaire pour ID {order_id}")
        return False
        
    except Exception as e:
        logger.error(f"Erreur lors de la vérification/mise à jour de la commande {order_id}: {str(e)}")
        # En cas d'erreur, on retourne True pour tenter une insertion
        return True

def send_data_to_supabase(processed_data):
    """
    Send data to Supabase using psycopg2 directly
    Returns a dictionary with statistics about the operations performed
    """
    # Compteurs pour les statistiques
    stats = {
        "inserted": 0,
        "updated": 0,
        "skipped": 0,
        "error": None
    }
    
    # Vérifier si des données ont été fournies
    if not processed_data or not isinstance(processed_data, list):
        stats["error"] = "Aucune donnée à traiter ou format incorrect"
        return stats
    
    logger.info(f"Tentative d'insertion de {len(processed_data)} commandes dans Supabase...")
    
    conn = None
    cur = None
    
    try:
        # Connexion à la base de données
        db_url = os.environ.get("DATABASE_URL")
        if not db_url:
            user = os.environ.get("SUPABASE_USER")
            password = os.environ.get("SUPABASE_PASSWORD")
            host = os.environ.get("SUPABASE_HOST")
            port = os.environ.get("SUPABASE_PORT")
            dbname = os.environ.get("SUPABASE_DB_NAME")
            
            # Vérifier que toutes les variables d'environnement nécessaires sont définies
            if not all([user, password, host, port, dbname]):
                error_msg = "Informations de connexion à la base de données incomplètes"
                logger.error(error_msg)
                stats["error"] = error_msg
                return stats
                
            db_url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
        
        # Établir la connexion avec un timeout
        conn = psycopg2.connect(db_url, connect_timeout=10)
        cur = conn.cursor()
        
        # Récupérer les types de colonnes
        query = """
        SELECT column_name, data_type, character_maximum_length
        FROM information_schema.columns 
        WHERE table_name = 'orders'
        """
        
        cur.execute(query)
        columns_info = cur.fetchall()
        
        if not columns_info:
            error_msg = "Impossible de récupérer les informations sur les colonnes de la table 'orders'"
            logger.error(error_msg)
            stats["error"] = error_msg
            return stats
        
        # Créer un dictionnaire des types de colonnes
        column_types = {}
        for column_name, data_type, max_length in columns_info:
            column_types[column_name] = {
                'type': data_type,
                'max_length': max_length
            }
        
        # Traiter les données
        for order in processed_data:
            try:
                # VÉRIFICATION CRITIQUE: S'assurer que la commande a un ID
                order_id = None
                for id_field in ['id', '_id_order', '_id']:
                    if id_field in order and order[id_field] is not None:
                        order_id = order[id_field]
                        break
                
                if order_id is None:
                    logger.warning(f"Commande ignorée: pas d'ID valide")
                    stats["skipped"] += 1
                    continue
                
                # Préparer les colonnes et les valeurs pour l'insertion
                columns = []
                values = []
                placeholders = []
                
                for key, value in order.items():
                    # Convertir les clés au format de la base de données (minuscules avec underscores)
                    column_name = key.lower().replace(" ", "_")
                    
                    # Vérifier si la colonne existe dans la table
                    if column_name not in column_types:
                        continue
                    
                    # Récupérer le type de la colonne
                    column_info = column_types[column_name]
                    column_type = column_info['type'].lower()
                    max_length = column_info['max_length']
                    
                    # Traiter la valeur en fonction du type de colonne
                    if value is None or value == "":
                        # Pour _id_order, ne jamais permettre de valeur NULL
                        if column_name == '_id_order':
                            continue
                        # Convertir les valeurs vides en NULL pour tous les types sauf text
                        if column_type != "text" and column_type != "character varying":
                            value = None
                    elif column_type in ["numeric", "decimal", "real", "double precision"]:
                        # Convertir en nombre si possible
                        try:
                            value = float(value) if value is not None else None
                        except (ValueError, TypeError):
                            value = None
                    elif column_type in ["integer", "bigint", "smallint"]:
                        # Convertir en entier si possible
                        try:
                            value = int(value) if value is not None else None
                        except (ValueError, TypeError):
                            value = None
                    elif column_type in ["timestamp", "timestamp without time zone", "timestamp with time zone", "date"]:
                        # Laisser les timestamps tels quels, ils seront gérés par psycopg2
                        pass
                    elif column_type == "boolean":
                        # Convertir en booléen
                        if isinstance(value, str):
                            value = value.lower() in ["true", "t", "yes", "y", "1"]
                    elif column_type == "character varying" and max_length is not None:
                        # Tronquer les chaînes trop longues
                        if isinstance(value, str) and len(value) > max_length:
                            value = value[:max_length]
                    
                    columns.append(column_name)
                    values.append(value)
                    placeholders.append("%s")
                
                # Vérification supplémentaire pour s'assurer que _id_order est présent
                if '_id_order' not in columns:
                    logger.warning(f"Commande ignorée: la colonne _id_order n'est pas parmi les colonnes à insérer")
                    stats["skipped"] += 1
                    continue
                
                if not columns:
                    # Aucune colonne valide trouvée, passer à la commande suivante
                    logger.warning(f"Commande ignorée: aucune colonne valide trouvée")
                    stats["skipped"] += 1
                    continue
                
                # Construire la requête SQL avec ON CONFLICT pour faire une upsert
                sql = f"""
                INSERT INTO orders ({', '.join(columns)}) 
                VALUES ({', '.join(placeholders)})
                ON CONFLICT (_id_order) DO UPDATE
                SET {', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col != '_id_order'])}
                RETURNING _id_order
                """
                
                # Exécuter la requête
                cur.execute(sql, values)
                result = cur.fetchone()
                
                if result is None or result[0] is None:
                    logger.warning(f"L'insertion n'a pas retourné d'ID pour la commande {order_id}")
                    stats["skipped"] += 1
                else:
                    inserted_id = result[0]
                    if str(inserted_id) == str(order_id):
                        stats["inserted"] += 1
                        logger.info(f"Commande {inserted_id} insérée avec succès")
                    else:
                        stats["updated"] += 1
                        logger.info(f"Commande {inserted_id} mise à jour avec succès")
                
                # Commit après chaque insertion pour éviter les problèmes de transaction trop longue
                conn.commit()
            
            except Exception as order_error:
                # Gérer les erreurs par commande sans interrompre le traitement complet
                error_msg = f"Erreur lors du traitement d'une commande: {order_error}"
                logger.error(error_msg)
                conn.rollback()  # Rollback pour cette commande seulement
                if not stats.get("error"):
                    stats["error"] = f"Erreur partielle: {str(order_error)}"
        
        logger.info(f"Statistiques de traitement: {stats['inserted']} commandes insérées, {stats['updated']} mises à jour, {stats['skipped']} ignorées.")
        
    except Exception as e:
        error_msg = f"Erreur lors de l'insertion des données: {e}"
        logger.error(error_msg)
        stats["error"] = error_msg
        if conn:
            try:
                conn.rollback()
            except:
                pass
    finally:
        if cur:
            try:
                cur.close()
            except:
                pass
        if conn:
            try:
                conn.close()
            except:
                pass
    
    return stats