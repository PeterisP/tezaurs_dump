# -*- coding: utf-8 -*-

# DB savienojuma konfigurācija
## lai konfigurētu DB savienojumu:
##  1) pārkopēt šo failu ar nosaukumu db_config.py
##  2) ievietot api_conn_info laukos atbilstošo savienojuma informāciju (hostname, ...)

db_connection_info = {
    "host":     "localhost", 
    "port":     5432, 
    "dbname":   "tezaurs", 
    
    "user":     "postgres", 
    "password": "",
}
