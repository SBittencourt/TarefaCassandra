import requests
import json

# Configurações do cliente
astra_database_id = "26d2f58b-da91-4410-bb44-ab1f51f90876"
astra_database_region = "us-east-2"
astra_application_token = "AstraCS:ytnWEwZaMJqFMYCvfwAPAYTl:79ce3b83a7d38b3cfd13302a23c4db9b52fc8757cbd010eae03a4da6195a06ec"

base_url = f"https://{astra_database_id}-{astra_database_region}.apps.astra.datastax.com/api/rest/v2/keyspaces"
headers = {
    "X-Cassandra-Token": astra_application_token,
    "Content-Type": "application/json"
}

# Função para criar keyspace
def create_keyspace():
    keyspace_name = "mercadolivre"
    url = f"{base_url}/{keyspace_name}"
    payload = {
        "name": keyspace_name,
        "datacenters": [
            {
                "name": "datacenter1",
                "replicas": 1
            }
        ]
    }
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    if response.status_code == 201 or response.status_code == 200:
        print("Keyspace criado com sucesso!")
    else:
        print(f"Falha ao criar keyspace: {response.text}")

# Função para criar tabela
def create_table():
    keyspace_name = "mercadolivre"
    table_name = "usuario"
    url = f"{base_url}/{keyspace_name}/tables"
    payload = {
        "name": table_name,
        "ifNotExists": True,
        "columnDefinitions": [
            {"name": "id", "typeDefinition": "uuid"},
            {"name": "nome", "typeDefinition": "text"},
            {"name": "sobrenome", "typeDefinition": "text"},
            {"name": "telefone", "typeDefinition": "text"},
            {"name": "email", "typeDefinition": "text"},
            {"name": "cpf", "typeDefinition": "text"},
            {"name": "enderecos", "typeDefinition": "frozen<list<map<text, text>>>"}
        ],
        "primaryKey": {
            "partitionKey": ["id"]
        }
    }
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    if response.status_code == 201 or response.status_code == 200:
        print("Tabela criada com sucesso!")
    else:
        print(f"Falha ao criar tabela: {response.text}")

# Criar keyspace e tabela
create_keyspace()
create_table()
