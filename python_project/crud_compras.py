from cassandra.cluster import Cluster, ConsistencyLevel
from cassandra.query import SimpleStatement
from cassandra import InvalidRequest
from cassandra.query import BatchStatement
import uuid

def realizar_compra(session, cpf_usuario):
    carrinho = []

    try:
        print("Lista de produtos disponíveis:")
        query_produto = SimpleStatement("SELECT id, nome, preco, vendedor FROM produto", consistency_level=ConsistencyLevel.LOCAL_QUORUM)
        produtos = list(session.execute(query_produto))

        for i, produto in enumerate(produtos, start=1):
            vendedor = session.execute("SELECT nome FROM vendedor WHERE cpf=%s ALLOW FILTERING", (produto.vendedor,)).one()
            if vendedor:
                print(f"{i} - ID: {produto.id} | Produto: {produto.nome} | Vendedor: {vendedor.nome} | Preço: {produto.preco}")
            else:
                print(f"{i} - ID: {produto.id} | Produto: {produto.nome} | Vendedor: Não disponível | Preço: {produto.preco}")

        while True:
            produto_id = input("\nDigite o ID do produto que deseja adicionar ao carrinho (ou 'F' para finalizar): ")
            if produto_id.upper() == 'F':
                break

            try:
                produto_id = int(produto_id)
                if 1 <= produto_id <= len(produtos):
                    produto_selecionado = produtos[produto_id - 1]
                    carrinho.append({
                        "nome": produto_selecionado.nome,
                        "preco": produto_selecionado.preco
                    })
                    print(f"Produto '{produto_selecionado.nome}' adicionado ao carrinho.")
                else:
                    print("ID de produto inválido.")
            except ValueError:
                print("Entrada inválida. Digite um número válido.")

        query_endereco = SimpleStatement("SELECT end FROM usuario WHERE cpf=%s", consistency_level=ConsistencyLevel.LOCAL_QUORUM)
        usuario = session.execute(query_endereco, (cpf_usuario,)).one()

        if not usuario or 'end' not in usuario or not usuario['end']:
            print("Usuário não possui endereço cadastrado. Vamos solicitar o endereço para continuar com a compra.")


            endereco_entrega = {}
            endereco_entrega['rua'] = input("Digite a rua: ")
            endereco_entrega['num'] = input("Digite o número: ")
            endereco_entrega['bairro'] = input("Digite o bairro: ")
            endereco_entrega['cidade'] = input("Digite a cidade: ")
            endereco_entrega['estado'] = input("Digite o estado: ")
            endereco_entrega['cep'] = input("Digite o CEP: ")

        else:
            endereco_entrega = usuario['end'][0]  

        print("\nEndereço selecionado para entrega:")
        print(f"{endereco_entrega['rua']}, {endereco_entrega['num']}, {endereco_entrega['bairro']}, {endereco_entrega['cidade']}, {endereco_entrega['estado']}, CEP: {endereco_entrega['cep']}")


        if not carrinho:
            print("Carrinho vazio. Operação cancelada.")
            return

        total = sum(float(produto['preco']) for produto in carrinho)
        print(f"\nValor total do carrinho: R${total:.2f}")
        confirmar = input("\nDeseja confirmar a compra (S/N)? ").upper()

        if confirmar != "S":
            print("Compra cancelada.")
            return


        batch = BatchStatement(consistency_level=ConsistencyLevel.LOCAL_QUORUM)

        compra_id = uuid.uuid4()
        for produto in carrinho:
            batch.add(
                "INSERT INTO compras (id, cpf_usuario, produtos, endereco_entrega, valor_total) VALUES (%s, %s, %s, %s, %s)",
                (compra_id, cpf_usuario, [{"nome": produto['nome'], "preco": str(produto['preco'])}], f"{endereco_entrega['rua']}, {endereco_entrega['num']}, {endereco_entrega['bairro']}, {endereco_entrega['cidade']}, {endereco_entrega['estado']}, CEP: {endereco_entrega['cep']}", total)
            )

        session.execute(batch)
        print("Compra concluída com sucesso!")
        return carrinho

    except InvalidRequest as e:
        print(f"Erro ao realizar compra: {e}")

def ver_compras_realizadas(session, cpf_usuario):
    try:
        if cpf_usuario:
            query_compras = SimpleStatement("SELECT * FROM compras WHERE cpf_usuario=%s ALLOW FILTERING", consistency_level=ConsistencyLevel.LOCAL_QUORUM)
            compras_realizadas = session.execute(query_compras, (cpf_usuario,))
        else:
            query_compras = SimpleStatement("SELECT * FROM compras ALLOW FILTERING", consistency_level=ConsistencyLevel.LOCAL_QUORUM)
            compras_realizadas = session.execute(query_compras)

        count = 0

        for compra in compras_realizadas:
            count += 1
            print(f"ID da Compra: {compra.id}")
            print(f"CPF do Usuário: {compra.cpf_usuario}") 
            print("Produtos:")
            for produto in compra.produtos:
                print(f"   Nome do Produto: {produto['nome']} | Preço: {produto['preco']}")
            print(f"Endereço de Entrega: {compra.endereco_entrega}")  
            print(f"Valor Total: R${compra.valor_total:.2f}")
            print("----")
        
        if count == 0:
            if cpf_usuario:
                print("Nenhuma compra encontrada para este usuário.")
            else:
                print("Nenhuma compra encontrada.")
    
    except InvalidRequest as e:
        print(f"Erro ao visualizar compras realizadas: {e}")

