import requests
import json
import uuid

# Configurações do cliente
astra_database_id = "26d2f58b-da91-4410-bb44-ab1f51f90876"
astra_database_region = "us-east-2"
astra_application_token = "AstraCS:ytnWEwZaMJqFMYCvfwAPAYTl:79ce3b83a7d38b3cfd13302a23c4db9b52fc8757cbd010eae03a4da6195a06ec"

base_url = f"https://{astra_database_id}-{astra_database_region}.apps.astra.datastax.com/api/rest/v2/keyspaces/mercadolivre"
headers = {
    "X-Cassandra-Token": astra_application_token,
    "Content-Type": "application/json"
}

def execute_cql(statement):
    url = f"{base_url}/cql"
    payload = {"query": statement}
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    if response.status_code == 201 or response.status_code == 200:
        return response
    else:
        print(f"Falha ao executar: {statement}")
        print(response.text)
        return None

def delete_usuario(nome, sobrenome):
    query = f"DELETE FROM usuario WHERE nome='{nome}' AND sobrenome='{sobrenome}';"
    execute_cql(query)
    print(f"Deletado o usuário {nome} {sobrenome}")

def create_usuario():
    id = str(uuid.uuid4())
    nomeUsuario = input("Nome: ")
    sobrenome = input("Sobrenome: ")
    telefone = input("Telefone: ")
    email = input("Email: ")
    cpf = input("CPF: ")
    end = []
    key = 'S'
    while key.upper() != 'N':
        rua = input("Rua: ")
        num = input("Num: ")
        bairro = input("Bairro: ")
        cidade = input("Cidade: ")
        estado = input("Estado: ")
        cep = input("CEP: ")
        endereco = {
            "rua": rua,
            "num": num,
            "bairro": bairro,
            "cidade": cidade,
            "estado": estado,
            "cep": cep
        }
        end.append(endereco)
        key = input("Deseja cadastrar um novo endereço (S/N)? ").upper()

    query = f"""
    INSERT INTO usuario (id, nome, sobrenome, telefone, email, cpf, enderecos) 
    VALUES ('{id}', '{nomeUsuario}', '{sobrenome}', '{telefone}', '{email}', '{cpf}', {json.dumps(end)});
    """
    execute_cql(query)
    print(f"Usuário {nomeUsuario} criado com sucesso com ID {id}")

def read_usuario(nomeUsuario=None):
    query = f"SELECT * FROM usuario WHERE nome='{nomeUsuario}';" if nomeUsuario else "SELECT * FROM usuario;"
    response = execute_cql(query)
    if response:
        usuarios = response.json()["data"]
        for user in usuarios:
            print(f"Nome: {user['nome']}")
            print(f"CPF: {user['cpf']}")
            print("Endereços:")
            for endereco in user["enderecos"]:
                print(f"Rua: {endereco['rua']}")
                print(f"Número: {endereco['num']}")
                print(f"Bairro: {endereco['bairro']}")
                print(f"Cidade: {endereco['cidade']}")
                print(f"Estado: {endereco['estado']}")
                print(f"CEP: {endereco['cep']}")
            visualizar_favoritos(user["cpf"])
            ver_compras_realizadas(user["cpf"])
            print("----")

def update_usuario(nomeUsuario):
    query = f"SELECT * FROM usuario WHERE nome='{nomeUsuario}';"
    response = execute_cql(query)
    if not response or not response.json()["data"]:
        print("Usuário não encontrado.")
        return
    
    user = response.json()["data"][0]
    print(f"Dados do usuário: {user}")

    print("\nMenu de opções:")
    print("1 - Mudar Nome")
    print("2 - Mudar Sobrenome")
    print("3 - Mudar CPF")
    print("4 - Mudar Telefone")
    print("5 - Mudar Email")
    print("6 - Mudar Endereço")
    print("7 - Voltar ao menu principal")

    while True:
        opcao = input("\nEscolha uma opção: ")

        if opcao == "1":
            nome = input("Novo Nome: ")
            if nome:
                user["nome"] = nome
        elif opcao == "2":
            sobrenome = input("Novo Sobrenome: ")
            if sobrenome:
                user["sobrenome"] = sobrenome
        elif opcao == "3":
            cpf = input("Novo CPF: ")
            if cpf:
                user["cpf"] = cpf
        elif opcao == "4":
            telefone = input("Novo Telefone: ")
            if telefone:
                user["telefone"] = telefone
        elif opcao == "5":
            email = input("Novo Email: ")
            if email:
                user["email"] = email
        elif opcao == "6":
            print("\nEndereço atual:")
            for endereco in user["enderecos"]:
                print(f"Rua: {endereco['rua']}")
                print(f"Número: {endereco['num']}")
                print(f"Bairro: {endereco['bairro']}")
                print(f"Cidade: {endereco['cidade']}")
                print(f"Estado: {endereco['estado']}")
                print(f"CEP: {endereco['cep']}")
            rua = input("\nNova Rua: ")
            num = input("Novo Número: ")
            bairro = input("Novo Bairro: ")
            cidade = input("Nova Cidade: ")
            estado = input("Novo Estado: ")
            cep = input("Novo CEP: ")
            endereco = {
                "rua": rua,
                "num": num,
                "bairro": bairro,
                "cidade": cidade,
                "estado": estado,
                "cep": cep
            }
            user["enderecos"] = [endereco]
        elif opcao == "7":
            print("Retornando ao menu principal...")
            break
        else:
            print("Opção inválida. Por favor, escolha uma opção válida.")

    update_query = f"""
    UPDATE usuario SET nome='{user['nome']}', sobrenome='{user['sobrenome']}', telefone='{user['telefone']}', email='{user['email']}', cpf='{user['cpf']}', enderecos={json.dumps(user['enderecos'])} 
    WHERE id={user['id']};
    """
    execute_cql(update_query)
    print("Usuário atualizado com sucesso!")
