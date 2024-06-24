import uuid
import connect_database

def create_produto(session):
    print("\nInserindo um novo produto")
    nomeProduto = input("Nome: ")
    preco = float(input("Preço: "))  
    marca = input("Marca: ")

    print("Vendedores existentes:")
    vendedores = session.execute("SELECT * FROM vendedor")
    for vendedor in vendedores:
        print("- Nome:", vendedor.nome, "| CPF:", vendedor.cpf)

    cpfVendedor = input("Digite o CPF do vendedor para associar ao produto: ")

    vendedor_existente = session.execute("SELECT * FROM vendedor WHERE cpf=%s", (cpfVendedor,)).one()
    if not vendedor_existente:
        print("Vendedor com o CPF", cpfVendedor, "não encontrado. Produto não foi criado.")
        return

    produto_id = uuid.uuid4()

    session.execute("""
        INSERT INTO produto (id, nome, preco, marca, vendedor) 
        VALUES (%s, %s, %s, %s, %s)
    """, (produto_id, nomeProduto, preco, marca, cpfVendedor))
    print("Produto inserido com sucesso. ID do Produto:", produto_id)

def read_produto(session, nomeProduto=None):
    if not nomeProduto:
        produtos = session.execute("SELECT * FROM produto")
        for produto in produtos:
            vendedor = session.execute("SELECT nome FROM vendedor WHERE cpf=%s", (produto.vendedor,)).one()
            vendedor_nome = vendedor.nome if vendedor else "Vendedor não encontrado"
            print(f"ID: {produto.id}, Nome: {produto.nome}, Preço: {produto.preco}, Marca: {produto.marca}, Vendedor: {vendedor_nome}")
    else:
        produto = session.execute("SELECT * FROM produto WHERE nome=%s ALLOW FILTERING", (nomeProduto,)).one()
        if produto:
            vendedor = session.execute("SELECT nome FROM vendedor WHERE cpf=%s", (produto.vendedor,)).one()
            vendedor_nome = vendedor.nome if vendedor else "Vendedor não encontrado"
            print(f"ID: {produto.id}, Nome: {produto.nome}, Preço: {produto.preco}, Marca: {produto.marca}, Vendedor: {vendedor_nome}")
        else:
            print(f"Produto com nome '{nomeProduto}' não encontrado.")

def update_produto(session, nomeProduto):
    produto = session.execute("SELECT * FROM produto WHERE nome=%s ALLOW FILTERING", (nomeProduto,)).one()
    if produto:
        print("Dados do produto: ")
        print(f"ID: {produto.id}, Nome: {produto.nome}, Preço: {produto.preco}, Marca: {produto.marca}")
        
        novo_nome = input("Mudar nome do produto: ")
        novo_preco = float(input("Mudar preço: "))
        nova_marca = input("Mudar marca: ")
        
        print("Vendedores existentes:")
        vendedores = session.execute("SELECT * FROM vendedor")
        for vendedor in vendedores:
            print("- Nome:", vendedor.nome, "| CPF:", vendedor.cpf)

        cpfVendedor = input("Digite o novo CPF do vendedor para associar ao produto: ")

        vendedor_existente = session.execute("SELECT * FROM vendedor WHERE cpf=%s", (cpfVendedor,)).one()
        if not vendedor_existente:
            print("Vendedor com o CPF", cpfVendedor, "não encontrado. Produto não foi atualizado.")
            return

        session.execute("""
            UPDATE produto SET nome=%s, preco=%s, marca=%s, vendedor=%s WHERE id=%s
        """, (novo_nome, novo_preco, nova_marca, cpfVendedor, produto.id))
        print("Produto atualizado com sucesso!")
    else:
        print("Produto não encontrado.")

def delete_produto(session, nomeProduto):
    produto = session.execute("SELECT * FROM produto WHERE nome=%s ALLOW FILTERING", (nomeProduto,)).one()
    if produto:
        session.execute("DELETE FROM produto WHERE id=%s", (produto.id,))
        print(f"Produto '{nomeProduto}' deletado com sucesso.")
    else:
        print(f"Produto '{nomeProduto}' não encontrado.")

if __name__ == "__main__":
    session = connect_database.create_session()

    # Teste de criação de produto
    print("\nTeste de criação de produto:")
    create_produto(session)

    # Teste de leitura de produto
    print("\nTeste de leitura de produto:")
    read_produto(session)

    # Teste de atualização de produto
    print("\nTeste de atualização de produto:")
    update_produto(session, "Produto Teste")

    # Teste de exclusão de produto
    print("\nTeste de exclusão de produto:")
    delete_produto(session, "Produto Teste")
