from cassandra.cluster import Cluster, ConsistencyLevel
from cassandra.query import SimpleStatement
from cassandra import InvalidRequest
from cassandra.query import BatchStatement
import uuid

def realizar_compra(session, cpf_usuario):
    carrinho = []

    try:
        # 1. Buscar produtos disponíveis
        print("Lista de produtos disponíveis:")
        query_produto = SimpleStatement("SELECT id, nome, preco, vendedor FROM produto", consistency_level=ConsistencyLevel.LOCAL_QUORUM)
        produtos = list(session.execute(query_produto))

        for i, produto in enumerate(produtos, start=1):
            vendedor = session.execute("SELECT nome FROM vendedor WHERE cpf=%s ALLOW FILTERING", (produto.vendedor,)).one()
            if vendedor:
                print(f"{i} - ID: {produto.id} | Produto: {produto.nome} | Vendedor: {vendedor.nome} | Preço: {produto.preco}")
            else:
                print(f"{i} - ID: {produto.id} | Produto: {produto.nome} | Vendedor: Não disponível | Preço: {produto.preco}")

        # 2. Buscar endereços do usuário
        query_enderecos = SimpleStatement("SELECT enderecos FROM usuario WHERE cpf=%s", consistency_level=ConsistencyLevel.LOCAL_QUORUM)
        usuario = session.execute(query_enderecos, (cpf_usuario,)).one()

        if not usuario or 'enderecos' not in usuario:
            print("Usuário não possui endereços cadastrados. Não é possível continuar com a compra.")
            return

        enderecos = usuario['enderecos']

        # Se houver apenas um endereço, seleciona automaticamente
        if len(enderecos) == 1:
            endereco_entrega = enderecos[0]
            print("\nEndereço selecionado para entrega:")
            print(f"{endereco_entrega['rua']}, {endereco_entrega['num']}, {endereco_entrega['bairro']}, {endereco_entrega['cidade']}, {endereco_entrega['estado']}, CEP: {endereco_entrega['cep']}")
        else:
            print("\nEndereços de entrega disponíveis:")
            for i, endereco in enumerate(enderecos, start=1):
                print(f"{i} - {endereco['rua']}, {endereco['num']}, {endereco['bairro']}, {endereco['cidade']}, {endereco['estado']}, CEP: {endereco['cep']}")

            while True:
                endereco_selecionado = input("Digite o número do endereço de entrega selecionado (ou 'C' para cancelar): ")
                if endereco_selecionado.upper() == 'C':
                    print("Operação cancelada.")
                    return

                try:
                    endereco_selecionado = int(endereco_selecionado)
                    if 1 <= endereco_selecionado <= len(enderecos):
                        endereco_entrega = enderecos[endereco_selecionado - 1]
                        print("\nEndereço selecionado para entrega:")
                        print(f"{endereco_entrega['rua']}, {endereco_entrega['num']}, {endereco_entrega['bairro']}, {endereco_entrega['cidade']}, {endereco_entrega['estado']}, CEP: {endereco_entrega['cep']}")
                        break
                    else:
                        print("Número de endereço inválido.")
                except ValueError:
                    print("Entrada inválida. Digite um número válido.")

        # 3. Selecionar produtos para o carrinho
        while True:
            produto_id = input("\nDigite o ID do produto que deseja adicionar ao carrinho (ou 'F' para finalizar): ")
            if produto_id.upper() == 'F':
                break

            try:
                produto_id = int(produto_id)
                if 1 <= produto_id <= len(produtos):
                    produto_selecionado = produtos[produto_id - 1]
                    carrinho.append({
                        "id": produto_selecionado.id,
                        "nome": produto_selecionado.nome,
                        "preco": produto_selecionado.preco
                    })
                    print(f"Produto '{produto_selecionado.nome}' adicionado ao carrinho.")
                else:
                    print("ID de produto inválido.")
            except ValueError:
                print("Entrada inválida. Digite um número válido.")

        # 4. Confirmar a compra
        if not carrinho:
            print("Carrinho vazio. Operação cancelada.")
            return

        total = sum(float(produto['preco']) for produto in carrinho)
        print(f"\nValor total do carrinho: R${total:.2f}")
        confirmar = input("\nDeseja confirmar a compra (S/N)? ").upper()

        if confirmar != "S":
            print("Compra cancelada.")
            return

        # 5. Inserir a compra no banco de dados
        batch = BatchStatement(consistency_level=ConsistencyLevel.LOCAL_QUORUM)

        compra_id = uuid.uuid4()
        for produto in carrinho:
            batch.add(
                "INSERT INTO compras (id, cpf_usuario, produtos, endereco_entrega, valor_total) VALUES (%s, %s, %s, %s, %s)",
                (compra_id, cpf_usuario, produto['nome'], f"{endereco_entrega['rua']}, {endereco_entrega['num']}, {endereco_entrega['bairro']}, {endereco_entrega['cidade']}, {endereco_entrega['estado']}, CEP: {endereco_entrega['cep']}", total)
            )

        session.execute(batch)
        print("Compra concluída com sucesso!")
        return carrinho

    except InvalidRequest as e:
        print(f"Erro ao realizar compra: {e}")

def ver_compras_realizadas(session, cpf_usuario):
    try:
        query_compras = SimpleStatement("SELECT * FROM compras WHERE cpf_usuario=%s ALLOW FILTERING", consistency_level=ConsistencyLevel.LOCAL_QUORUM)
        compras_realizadas = session.execute(query_compras, (cpf_usuario,))
        
        count = 0

        for compra in compras_realizadas:
            count += 1
            print(f"ID da Compra: {compra.id}")
            print("Produtos:")
            for produto in compra.produtos:
                print(f"   Nome do Produto: {produto['nome']} | Preço: {produto['preco']}")
            print(f"Endereço de Entrega: {compra.endereco_entrega}")
            print(f"Valor Total: R${compra.valor_total:.2f}")
            print("----")
        
        if count == 0:
            print("Nenhuma compra encontrada para este usuário.")
    
    except InvalidRequest as e:
        print(f"Erro ao visualizar compras realizadas: {e}")

def detalhar_compra(session, compra_id):
    try:
        query_compra = SimpleStatement("SELECT * FROM compras WHERE id=%s ALLOW FILTERING", consistency_level=ConsistencyLevel.LOCAL_QUORUM)
        compra = session.execute(query_compra, (compra_id,)).one()

        if compra:
            print(f"ID da Compra: {compra.id}")
            print("Produtos:")
            for produto in compra.produtos:
                print(f"   Nome do Produto: {produto['nome']} | Preço: {produto['preco']}")
            print(f"Endereço de Entrega: {compra.endereco_entrega}")
            print(f"Valor Total: R${compra.valor_total:.2f}")
        else:
            print(f"Compra com ID '{compra_id}' não encontrada.")
    
    except InvalidRequest as e:
        print(f"Erro ao detalhar compra: {e}")
