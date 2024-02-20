from neo4j import GraphDatabase
import csv

uri = "bolt://localhost:7687"
username = "neo4j"
password = "1212121212B"

driver = GraphDatabase.driver(uri, auth=(username, password))

def delete_all(tx):
    query = "MATCH (n) DETACH DELETE n"
    tx.run(query)

def create_region(tx, data):
    query = """
    MERGE (r:Region {code_region: $code_region})
    SET r.nom_region = $nom_region
    """
    tx.run(query, **data)

def create_department(tx, data):
    query = """
    MATCH (r:Region {code_region: $code_region})
    MERGE (d:Departement {code_departement: $code_departement})
    SET d.nom_departement = $nom_departement,
        d.code_region = $code_region,
        d.nom_region = $nom_region
    MERGE (r)-[:HAS_DEPARTMENT]->(d)
    """
    tx.run(query, **data)

def create_commune(tx, data):
    query = """
    MATCH (d:Departement {code_departement: $code_departement})
    MERGE (c:Commune {code_commune_INSEE: $code_commune_INSEE})
    SET c.nom_commune = $nom_commune,
        c.code_postal = $code_postal,
        c.libelle_acheminement = $libelle_acheminement,
        c.ligne_5 = $ligne_5,
        c.latitude = $latitude,
        c.longitude = $longitude,
        c.code_commune = $code_commune,
        c.article = $article,
        c.nom_commune_complet = $nom_commune_complet,
        c.code_departement = $code_departement,
        c.nom_departement = $nom_departement,
        c.code_region = $code_region,
        c.nom_region = $nom_region
    MERGE (d)-[:HAS_COMMUNE]->(c)
    """
    tx.run(query, **data)

def create_equipment(tx, data):
    query = """
    MERGE (e:Equipment {name: $name})
    SET e.type = $type
    """
    tx.run(query, **data)

def create_accounting(tx, data):
    query = """
    MERGE (a:Accounting {year: $year})
    SET a.balance = $balance
    """
    tx.run(query, **data)

def create_mayor(tx, data):
    query = """
    MERGE (m:Mayor {name: $name})
    """
    tx.run(query, **data)

def create_resident(tx, data):
    query = """
    MERGE (r:Resident {name: $nom_habitant})
    SET r.population = $population
    """
    tx.run(query, **data)

def link_commune_to_information(tx, data):
    query = """
    MATCH (c:Commune {code_commune_INSEE: $code_commune_INSEE})
    MERGE (c)-[:HAS_EQUIPMENT]->(e)
    MERGE (c)-[:HAS_ACCOUNTING]->(a)
    MERGE (c)-[:HAS_MAYOR]->(m)
    MERGE (c)-[:HAS_RESIDENT]->(r)
    """
    tx.run(query, **data)

with driver.session() as session:
    # Supprimer tous les nœuds et relations existants
    session.write_transaction(delete_all)
    
    with open('communes-departement-region.csv', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if 'code_region' in row:
                print("Create a region")
                session.write_transaction(create_region, row)
            if 'code_departement' in row:
                print("Create a departement")
                session.write_transaction(create_department, row)
            if 'code_commune' in row:
                print("Create a commune")
                session.write_transaction(create_commune, row)
            # Creatin of a posible structure for aditional data 
            session.write_transaction(link_commune_to_information, row)

    print("Nodes created and linked with additional information")
