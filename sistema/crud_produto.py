import uuid
from astrapy.rest import create_client, request

client = create_client("AstraCS:ytnWEwZaMJqFMYCvfwAPAYTl:79ce3b83a7d38b3cfd13302a23c4db9b52fc8757cbd010eae03a4da6195a06ec")
namespace = "mercadolivre"
base_url = f"https://26d2f58b-da91-4410-bb44-ab1f51f90876-us-east-2.apps.astra.datastax.com/api/rest/v2/namespaces/{namespace}"

def delete_produto(nomeProduto): 
    query = {
        "query": f"DELETE FROM produtos WHERE nome='{nomeProduto}'"
    }
    response = request(
        method="POST",
        url=f"{base_url}/collections/produtos",
        headers=client.headers,
        json=query
    )
    if response.status_code == 200:
        print("Produto deletado:", nomeProduto)
    else:
        print("Erro ao deletar o produto:", response.json())

def create_produto():
    id = str(uuid.uuid4())
    nomeProduto = input("Nome: ")
    preco = input("Preço: ")
    marca = input("Marca: ")

    response = request(
        method="GET",
        url=f"{base_url}/collections/vendedores",
        headers=client.headers
    )
    vendedores = response.json()["data"]

    print("Vendedores existentes:")
    for vendedor in vendedores:
        print("- Nome:", vendedor["nome"], "| CPF:", vendedor["cpf"])

    cpfVendedor = input("Digite o CPF do vendedor para associar ao produto: ")

    vendedor_existente = any(v["cpf"] == cpfVendedor for v in vendedores)
    if not vendedor_existente:
        print("Vendedor com o CPF", cpfVendedor, "não encontrado. Produto não foi criado.")
        return

    payload = {
        "id": id,
        "nome": nomeProduto,
        "preco": float(preco),
        "marca": marca,
        "vendedor": cpfVendedor
    }
    response = request(
        method="POST",
        url=f"{base_url}/collections/produtos",
        headers=client.headers,
        json=payload
    )
    if response.status_code == 201:
        print("Produto inserido com ID:", id)
    else:
        print("Erro ao inserir o produto:", response.json())

def read_produto(nomeProduto):
    print("Produtos existentes: ")
    query = {"query": f"SELECT * FROM produtos WHERE nome='{nomeProduto}'"} if nomeProduto else {"query": "SELECT * FROM produtos"}
    response = request(
        method="POST",
        url=f"{base_url}/collections/produtos",
        headers=client.headers,
        json=query
    )
    produtos = response.json()["data"]
    for produto in produtos:
        print(f"Nome: {produto['nome']}, Preço: {produto['preco']}, Marca: {produto['marca']}, Vendedor: {produto['vendedor']}")

def update_produto(nomeProduto):
    query = {"query": f"SELECT * FROM produtos WHERE nome='{nomeProduto}'"}
    response = request(
        method="POST",
        url=f"{base_url}/collections/produtos",
        headers=client.headers,
        json=query
    )
    produtos = response.json()["data"]
    if produtos:
        produto = produtos[0]
        print("Dados do produto:", produto)

        novo_nome = input("Mudar nome do produto:")
        if novo_nome:
            produto["nome"] = novo_nome

        novo_preco = input("Mudar preço:")
        if novo_preco:
            produto["preco"] = float(novo_preco)

        nova_marca = input("Mudar marca:")
        if nova_marca:
            produto["marca"] = nova_marca

        response = request(
            method="GET",
            url=f"{base_url}/collections/vendedores",
            headers=client.headers
        )
        vendedores = response.json()["data"]

        print("Vendedores existentes:")
        for vendedor in vendedores:
            print("- Nome:", vendedor["nome"], "| CPF:", vendedor["cpf"])

        cpfVendedor = input("Digite o novo CPF do vendedor para associar ao produto: ")

        vendedor_existente = any(v["cpf"] == cpfVendedor for v in vendedores)
        if not vendedor_existente:
            print("Vendedor com o CPF", cpfVendedor, "não encontrado. Produto não foi atualizado.")
            return

        produto["vendedor"] = cpfVendedor

        update_query = {
            "query": f"UPDATE produtos SET nome='{produto['nome']}', preco={produto['preco']}, marca='{produto['marca']}', vendedor='{produto['vendedor']}' WHERE id={produto['id']}"
        }
        update_response = request(
            method="POST",
            url=f"{base_url}/collections/produtos",
            headers=client.headers,
            json=update_query
        )
        if update_response.status_code == 200:
            print("Produto atualizado com sucesso!")
        else:
            print("Erro ao atualizar o produto:", update_response.json())
    else:
        print("Produto não encontrado.")
