# Firebird DB to PostgreSQL DB
Firebird DB is a local DB, but PostgresSQL DB is located in Azure Cloud. Originally I was thinking to connect to PostgreSQL DB in Azure with python, which is do-able theoretically, then I realized to use config API returns (all nodes and all relations under encSystem) is a much better approach, due to 2 reasons:

- easier to implement
- verify 2 things at the same time, PostgreSQL DB and BIM Config API at the same time.

Firebird DB has 64 tables, but PostgreSQL DB only has 2 tables: node table and relation table, so the tables relationship between Firebird and PostgreSQL is not 1 to 1 relationship, so we can not use the method I used before when we migrate Firebird DB to PostgreSQL DB anymore. 

The data validation between Firebird DB and PostgreSQL DB was based on a BIM Objects file and ObjectType file provided by developer, which is not attached to this repo due to privacy reasons, however, the BIM Objects file explained all available nodes and relations in the PostgreSQL DB and where to find them in the Firebird DB tables, the ObjectType file specified in the same Firebird DB table, which may contains multiple types of nodes, to use the ObjectType filter, you can narrow down to the node that you are looking for. 