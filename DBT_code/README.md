## 1. Utiliser l'Incremental pour vos Tables de Faits (Transactions)
C'est votre outil numéro 1 pour la volumétrie. Sur des tables contenant des millions ou milliards de lignes (logs, événements, transactions), faire un dbt run complet chaque jour est impossible.
• Pourquoi : Vous ne traitez que les quelques milliers de lignes arrivées depuis la dernière exécution.
• Astuce Performance : Combinez l'incremental avec le Partitionnement (sur BigQuery/Snowflake/Databricks) pour que dbt n'aille scanner que la partition de données la plus récente.

## 2. Utiliser les Snapshots pour vos Référentiels (Dimensions)
Même avec beaucoup de données, on n'utilise généralement pas de snapshots sur des tables de transactions. On les réserve aux Dimensions (Clients, Produits, Magasins) qui changent.
• Le danger du volume : Un snapshot sur une table de 100 millions de lignes qui change constamment peut devenir très lourd, car dbt doit comparer chaque ligne pour détecter les changements.
• La solution : Si le volume est trop grand pour un snapshot classique, on préfère souvent une approche "Append-only" où la source nous envoie déjà les versions, ou on utilise le mécanisme de Change Data Capture (CDC) fourni par l'outil d'ingestion (comme Fivetran ou Airbyte).

Inventaire :
    -- Incremental (Faits) : payments, trips
    -- Snapshot (Dimensions) : customers, drivers, vehicules, locations