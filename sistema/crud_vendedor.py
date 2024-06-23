import uuid
from astrapy.rest import create_client, request

client = create_client("AstraCS:ytnWEwZaMJqFMYCvfwAPAYTl:79ce3b83a7d38b3cfd13302a23c4db9b52fc8757cbd010eae03a4da6195a06ec")
namespace = "mercadolivre"

def delete_vendedor(nome, cpf):
    query = {
        "query": f"DELETE FROM vendedores WHERE nome='{nome}' AND cpf='{cpf}'"
    }
    response = request(
        method="POST",
        url=f"{base_url}/collections/vendedores",
        headers=client.headers,
        json=query
    )
    print(f"Vendedor deletado: {nome} {cpf}")

def create_vendedor():
    id = str(uuid.uuid4())
    nome = input("Nome: ")
    cpf = input("CPF: ")
    telefone = input("Telefone: ")
    email = input("Email: ")

    enderecos = []
    key = 'S'
    while key.upper() == 'S':
        rua = input("Rua: ")
        num = input("Número: ")
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
        enderecos.append(endereco)
        key = input("Deseja adicionar outro endereço (S/N)? ")

    payload = {
        "id": id,
        "nome": nome,
        "cpf": cpf,
        "telefone": telefone,
        "email": email,
        "enderecos": enderecos
    }
    response = request(
        method="POST",
        url=f"{base_url}/collections/vendedores",
        headers=client.headers,
        json=payload
    )
    print(f"Vendedor inserido com ID: {id}")

def read_vendedor(nome):
    print("Vendedores existentes:")
    query = {"query": f"SELECT * FROM vendedores WHERE nome='{nome}'"} if nome else {"query": "SELECT * FROM vendedores"}
    response = request(
        method="POST",
        url=f"{base_url}/collections/vendedores",
        headers=client.headers,
        json=query
    )
    vendedores = response.json()
    for vendedor in vendedores:
        print(f"Nome: {vendedor['nome']}, CPF: {vendedor['cpf']}")
        for endereco in vendedor["enderecos"]:
            print(f"  Rua: {endereco['rua']}, Número: {endereco['num']}, Bairro: {endereco['bairro']}, Cidade: {endereco['cidade']}, Estado: {endereco['estado']}, CEP: {endereco['cep']}")

def update_vendedor(nome):
    query = {"query": f"SELECT * FROM vendedores WHERE nome='{nome}'"}
    response = request(
        method="POST",
        url=f"{base_url}/collections/vendedores",
        headers=client.headers,
        json=query
    )
    vendedor = response.json()[0]
    print(f"Dados do vendedor: {vendedor}")

    novo_nome = input("Novo nome: ")
    if novo_nome:
        vendedor["nome"] = novo_nome

    cpf = input("Novo CPF: ")
    if cpf:
        vendedor["cpf"] = cpf

    telefone = input("Novo telefone: ")
    if telefone:
        vendedor["telefone"] = telefone

    email = input("Novo email: ")
    if email:
        vendedor["email"] = email

    update_query = {
        "query": f"UPDATE vendedores SET nome='{vendedor['nome']}', cpf='{vendedor['cpf']}', telefone='{vendedor['telefone']}', email='{vendedor['email']}', enderecos={vendedor['enderecos']} WHERE id={vendedor['id']}"
    }
    update_response = request(
        method="POST",
        url=f"{base_url}/collections/vendedores",
        headers=client.headers,
        json=update_query
    )
    print("Vendedor atualizado com sucesso!")
