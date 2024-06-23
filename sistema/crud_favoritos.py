from astrapy.rest import create_client, request
import uuid

client = create_client("AstraCS:ytnWEwZaMJqFMYCvfwAPAYTl:79ce3b83a7d38b3cfd13302a23c4db9b52fc8757cbd010eae03a4da6195a06ec")
namespace = "mercadolivre"
base_url = f"https://26d2f58b-da91-4410-bb44-ab1f51f90876-us-east-2.apps.astra.datastax.com/api/rest/v2/namespaces/{namespace}"

def adicionar_favorito(cpf_usuario, id_produto):
    usuario_query = {"query": f"SELECT * FROM usuarios WHERE cpf='{cpf_usuario}'"}
    produto_query = {"query": f"SELECT * FROM produtos WHERE id={id_produto}"}

    usuario_response = request(
        method="POST",
        url=f"{base_url}/collections/usuarios",
        headers=client.headers,
        json=usuario_query
    )

    produto_response = request(
        method="POST",
        url=f"{base_url}/collections/produtos",
        headers=client.headers,
        json=produto_query
    )

    if not usuario_response.json()["data"]:
        print("Erro: Usuário não encontrado.")
        return

    if not produto_response.json()["data"]:
        print("Erro: Produto não encontrado.")
        return

    # Adiciona o favorito à coleção de favoritos
    favorito_payload = {
        "cpf_usuario": cpf_usuario,
        "id_produto": id_produto
    }
    favorito_response = request(
        method="POST",
        url=f"{base_url}/collections/favoritos",
        headers=client.headers,
        json=favorito_payload
    )

    if favorito_response.status_code == 201:
        print("Produto adicionado aos favoritos do usuário com sucesso!")
    else:
        print("Erro ao adicionar produto aos favoritos:", favorito_response.json())

def visualizar_favoritos(cpf_usuario):
    favorito_query = {"query": f"SELECT * FROM favoritos WHERE cpf_usuario='{cpf_usuario}'"}
    favorito_response = request(
        method="POST",
        url=f"{base_url}/collections/favoritos",
        headers=client.headers,
        json=favorito_query
    )

    favoritos = favorito_response.json()["data"]
    print("Favoritos do usuário:")
    for favorito in favoritos:
        produto_query = {"query": f"SELECT * FROM produtos WHERE id={favorito['id_produto']}"}
        produto_response = request(
            method="POST",
            url=f"{base_url}/collections/produtos",
            headers=client.headers,
            json=produto_query
        )

        produto = produto_response.json()["data"]
        if produto:
            vendedor_query = {"query": f"SELECT * FROM vendedores WHERE cpf='{produto['vendedor']}'"}
            vendedor_response = request(
                method="POST",
                url=f"{base_url}/collections/vendedores",
                headers=client.headers,
                json=vendedor_query
            )

            vendedor = vendedor_response.json()["data"]
            if vendedor:
                print("Nome do Produto:", produto["nome"])
                print("Preço:", produto["preco"])
                print("Vendedor:", vendedor["nome"])
                print()
            else:
                print("Vendedor não encontrado para o produto:", produto["nome"])
        else:
            print("Produto não encontrado para o favorito com ID:", favorito["id_produto"])

def excluir_favorito(cpf_usuario):
    favorito_query = {"query": f"SELECT * FROM favoritos WHERE cpf_usuario='{cpf_usuario}'"}
    favorito_response = request(
        method="POST",
        url=f"{base_url}/collections/favoritos",
        headers=client.headers,
        json=favorito_query
    )

    favoritos = favorito_response.json()["data"]
    if favoritos:
        print("Favoritos do usuário:")
        for i, favorito in enumerate(favoritos, start=1):
            produto_query = {"query": f"SELECT * FROM produtos WHERE id={favorito['id_produto']}"}
            produto_response = request(
                method="POST",
                url=f"{base_url}/collections/produtos",
                headers=client.headers,
                json=produto_query
            )

            produto = produto_response.json()["data"]
            if produto:
                vendedor_query = {"query": f"SELECT * FROM vendedores WHERE cpf='{produto['vendedor']}'"}
                vendedor_response = request(
                    method="POST",
                    url=f"{base_url}/collections/vendedores",
                    headers=client.headers,
                    json=vendedor_query
                )

                vendedor = vendedor_response.json()["data"]
                if vendedor:
                    print(f"{i} - Nome do Produto: {produto['nome']} | Preço: {produto['preco']} | Vendedor: {vendedor['nome']}")
                else:
                    print(f"{i} - Nome do Produto: {produto['nome']} | Preço: {produto['preco']} | Vendedor: Não disponível")
            else:
                print(f"{i} - Produto não encontrado para o favorito com ID: {favorito['id_produto']}")

        while True:
            try:
                indice = int(input("Digite o número do favorito que deseja excluir (ou '0' para cancelar): "))
                if indice == 0:
                    print("Operação cancelada.")
                    return
                elif 1 <= indice <= len(favoritos):
                    break
                else:
                    print("Número de favorito inválido. Digite um número válido.")
            except ValueError:
                print("Entrada inválida. Digite um número válido.")

        favorito_selecionado = favoritos[indice - 1]
        delete_query = {"query": f"DELETE FROM favoritos WHERE cpf_usuario='{cpf_usuario}' AND id_produto={favorito_selecionado['id_produto']}"}
        delete_response = request(
            method="POST",
            url=f"{base_url}/collections/favoritos",
            headers=client.headers,
            json=delete_query
        )

        if delete_response.status_code == 200:
            print("Favorito removido com sucesso!")
        else:
            print("Erro ao remover favorito:", delete_response.json())
    else:
        print("O usuário não possui favoritos.")

def listar_produtos():
    produtos_query = {"query": "SELECT * FROM produtos"}
    produtos_response = request(
        method="POST",
        url=f"{base_url}/collections/produtos",
        headers=client.headers,
        json=produtos_query
    )

    produtos = produtos_response.json()["data"]
    print("Lista de produtos:")
    for i, produto in enumerate(produtos, start=1):
        vendedor_query = {"query": f"SELECT * FROM vendedores WHERE cpf='{produto['vendedor']}'"}
        vendedor_response = request(
            method="POST",
            url=f"{base_url}/collections/vendedores",
            headers=client.headers,
            json=vendedor_query
        )

        vendedor = vendedor_response.json()["data"]
        if vendedor:
            print(f"{i} - ID: {produto['id']} | Produto: {produto['nome']} | Vendedor: {vendedor['nome']} | Preço: {produto['preco']}")
        else:
            print(f"{i} - ID: {produto['id']} | Produto: {produto['nome']} | Vendedor: Não disponível | Preço: {produto['preco']}")

def adicionarnovo_favorito():
    while True:
        cpf_usuario = input("Digite o CPF do usuário: ")
        usuario_query = {"query": f"SELECT * FROM usuarios WHERE cpf='{cpf_usuario}'"}
        usuario_response = request(
            method="POST",
            url=f"{base_url}/collections/usuarios",
            headers=client.headers,
            json=usuario_query
        )

        if not usuario_response.json()["data"]:
            print("Erro: CPF de usuário não encontrado.")
            break

        print("\nLista de produtos:")
        listar_produtos()

        id_produto = input("Digite o número do produto que deseja adicionar aos favoritos (ou 'V' para voltar): ")
        if id_produto.upper() == 'V':
            return

        try:
            id_produto = int(id_produto)
            produto_query = {"query": "SELECT * FROM produtos"}
            produto_response = request(
                method="POST",
                url=f"{base_url}/collections/produtos",
                headers=client.headers,
                json=produto_query
            )

            produtos = produto_response.json()["data"]
            if id_produto < 1 or id_produto > len(produtos):
                raise ValueError
        except ValueError:
            print("Erro: Entrada inválida. Digite um número válido.")
            continue

        produto_selecionado = produtos[id_produto - 1]
        adicionar_favorito(cpf_usuario, produto_selecionado["id"])

        adicionar_mais = input("Deseja adicionar mais produtos aos favoritos (S/N)? ").upper()
        if adicionar_mais != "S":
            break
