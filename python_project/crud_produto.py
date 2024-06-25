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

def read_produto(session, cpfVendedor=None):
    if not cpfVendedor:
        produtos = session.execute("SELECT * FROM produto")
    else:
        produtos = session.execute("SELECT * FROM produto WHERE vendedor=%s ALLOW FILTERING", (cpfVendedor,))

    for produto in produtos:
        vendedor = session.execute("SELECT nome FROM vendedor WHERE cpf=%s", (produto.vendedor,)).one()
        vendedor_nome = vendedor.nome if vendedor else "Vendedor não encontrado"
        print(f"ID: {produto.id}, Nome: {produto.nome}, Preço: {produto.preco}, Marca: {produto.marca}, Vendedor: {vendedor_nome}")

def update_produto(session, produto_id):
    try:
        produto_uuid = uuid.UUID(produto_id)
    except ValueError:
        print("ID do produto inválido. Certifique-se de que está correto.")
        return

    produto = session.execute("SELECT * FROM produto WHERE id=%s", (produto_uuid,)).one()
    if produto:
        print("Dados do produto: ")
        print(f"ID: {produto.id}, Nome: {produto.nome}, Preço: {produto.preco}, Marca: {produto.marca}")
        
        novo_nome = input("Mudar nome do produto (Pressione Enter para manter o atual): ")
        novo_preco = input("Mudar preço (Pressione Enter para manter o atual): ")
        nova_marca = input("Mudar marca (Pressione Enter para manter o atual): ")
        
        print("Vendedores existentes:")
        vendedores = session.execute("SELECT * FROM vendedor")
        for vendedor in vendedores:
            print("- Nome:", vendedor.nome, "| CPF:", vendedor.cpf)

        cpfVendedor = input("Digite o novo CPF do vendedor para associar ao produto (Pressione Enter para manter o atual): ")

        vendedor_existente = None
        if cpfVendedor:
            vendedor_existente = session.execute("SELECT * FROM vendedor WHERE cpf=%s", (cpfVendedor,)).one()
            if not vendedor_existente:
                print("Vendedor com o CPF", cpfVendedor, "não encontrado. Produto não foi atualizado.")
                return

        session.execute("""
            UPDATE produto SET nome=%s, preco=%s, marca=%s, vendedor=%s WHERE id=%s
        """, (
            novo_nome if novo_nome else produto.nome,
            float(novo_preco) if novo_preco else produto.preco,
            nova_marca if nova_marca else produto.marca,
            cpfVendedor if cpfVendedor else produto.vendedor,
            produto_uuid
        ))
        print("Produto atualizado com sucesso!")
    else:
        print("Produto não encontrado.")

def delete_produto(session, produto_id):
    try:
        produto_uuid = uuid.UUID(produto_id)
    except ValueError:
        print("ID do produto inválido. Certifique-se de que está correto.")
        return

    produto = session.execute("SELECT * FROM produto WHERE id=%s", (produto_uuid,)).one()
    if produto:
        session.execute("DELETE FROM produto WHERE id=%s", (produto_uuid,))
        print(f"Produto com ID '{produto_id}' deletado com sucesso.")
    else:
        print(f"Produto com ID '{produto_id}' não encontrado.")

if __name__ == "__main__":
    session = connect_database.create_session()

    # Teste de criação de produto
    print("\nTeste de criação de produto:")
    create_produto(session)

    # Teste de leitura de produto
    print("\nTeste de leitura de produto:")
    cpfVendedor = input("Digite o CPF do vendedor para visualizar seus produtos (Pressione Enter para ver todos os produtos): ")
    read_produto(session, cpfVendedor if cpfVendedor else None)

    # Teste de atualização de produto
    produto_id = input("\nDigite o ID do produto que deseja atualizar: ")
    update_produto(session, produto_id)

    # Teste de exclusão de produto
    print("\nTeste de exclusão de produto:")
    produto_id = input("Digite o ID do produto a ser deletado: ")
    delete_produto(session, produto_id)
