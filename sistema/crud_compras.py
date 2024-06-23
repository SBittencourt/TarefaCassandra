from astrapy.rest import create_client
import uuid

client = create_client("AstraCS:ytnWEwZaMJqFMYCvfwAPAYTl:79ce3b83a7d38b3cfd13302a23c4db9b52fc8757cbd010eae03a4da6195a06ec")
base_url = "https://26d2f58b-da91-4410-bb44-ab1f51f90876-us-east-2.apps.astra.datastax.com/api/rest/v2/namespaces/mercadolivre"

def realizar_compra(cpf_usuario):
    carrinho = []

    print("Lista de produtos disponíveis:")
    response = client.get(f"{base_url}/collections/produto")
    produtos = response.json()['data']

    for i, produto in enumerate(produtos, start=1):
        vendedor_response = client.get(f"{base_url}/collections/vendedor/{produto['vendedor']}")
        vendedor = vendedor_response.json()
        if vendedor:
            print(f"{i} - ID: {produto['id']} | Produto: {produto['nome']} | Vendedor: {vendedor['nome']} | Preço: {produto['preco']}")
        else:
            print(f"{i} - ID: {produto['id']} | Produto: {produto['nome']} | Vendedor: Não disponível | Preço: {produto['preco']}")

    while True:
        id_produto = input("\nDigite o número do produto que deseja adicionar ao carrinho (ou 'C' para concluir): ")
        if id_produto.upper() == 'C':
            break

        try:
            id_produto = int(id_produto)
            if id_produto < 1 or id_produto > len(produtos):
                raise ValueError
            produto = produtos[id_produto - 1]
            carrinho.append(produto)
            print(f"Produto '{produto['nome']}' adicionado ao carrinho.")
        except ValueError:
            print("Erro: Produto inválido. Digite um número válido.")

    if not carrinho:
        print("Carrinho vazio. Operação cancelada.")
        return
        
    total = sum(produto["preco"] for produto in carrinho)

    # Mostra o valor total ao usuário
    print(f"\nValor total do carrinho: R${total:.2f}")

    # Pede confirmação antes de finalizar a compra
    confirmar = input("\nDeseja confirmar a compra (S/N)? ").upper()
    if confirmar != "S":
        print("Compra cancelada.")
        return carrinho

    response = client.get(f"{base_url}/collections/usuario/{cpf_usuario}")
    usuario = response.json()
    if usuario:
        enderecos = usuario.get("end", [])
        print("\nSelecione o endereço de entrega:")
        for i, endereco in enumerate(enderecos, start=1):
            print(f"{i} - {endereco['rua']}, {endereco['num']}, {endereco['bairro']}, {endereco['cidade']}, {endereco['estado']}, CEP: {endereco['cep']}")
        
        while True:
            endereco_selecionado = input("Digite o número do endereço selecionado: ")
            try:
                endereco_selecionado = int(endereco_selecionado)
                if 1 <= endereco_selecionado <= len(enderecos):
                    endereco_entrega = enderecos[endereco_selecionado - 1]
                    print("Endereço selecionado para entrega:")
                    print(f"{endereco_entrega['rua']}, {endereco_entrega['num']}, {endereco_entrega['bairro']}, {endereco_entrega['cidade']}, {endereco_entrega['estado']}, CEP: {endereco_entrega['cep']}")
                    
                    # Inserir a compra no banco de dados
                    compra = {
                        "id": str(uuid.uuid4()),
                        "cpf_usuario": cpf_usuario,
                        "produtos": carrinho,
                        "endereco_entrega": endereco_entrega,
                        "valor_total": total
                    }
                    client.post(f"{base_url}/collections/compras", json=compra)
                    
                    print("Compra concluída com sucesso!")
                    return carrinho
                else:
                    print("Número de endereço inválido.")
            except ValueError:
                print("Entrada inválida. Digite um número válido.")
    else:
        print("Usuário não encontrado. Não é possível continuar com a compra.")
        return carrinho

def ver_compras_realizadas(cpf_usuario):
    print("Compras realizadas pelo usuário:")
    
    response = client.get(f"{base_url}/collections/compras")
    compras_realizadas = response.json()['data']
    
    count = 0

    for compra in compras_realizadas:
        if compra['cpf_usuario'] == cpf_usuario:
            count += 1
            print(f"ID da Compra: {compra['id']}")
            print("Produtos:")
            for produto in compra['produtos']:
                print(f"   Nome do Produto: {produto['nome']} | Preço: {produto['preco']}")
            print(f"Endereço de Entrega: {compra['endereco_entrega']}")
            print("----")
    
    if count == 0:
        print("Nenhuma compra encontrada para este usuário.")
