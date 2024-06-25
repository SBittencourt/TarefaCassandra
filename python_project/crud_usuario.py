from cassandra.cluster import Cluster, ConsistencyLevel
from cassandra.query import SimpleStatement

def delete_usuario(session, cpf_usuario):
    query = SimpleStatement("DELETE FROM usuario WHERE cpf=%s", consistency_level=ConsistencyLevel.LOCAL_QUORUM)
    session.execute(query, (cpf_usuario,))
    print(f"Usuário com CPF '{cpf_usuario}' deletado com sucesso.")

def create_tables(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS usuario (
        nome text,
        sobrenome text,
        cpf text PRIMARY KEY,
        telefone text,
        email text,
        end list<frozen<map<text, text>>>,
        favorito list<text>
    )
    """)

    # Defina outras tabelas aqui (vendedor, produto, favoritos, compras)

def create_usuario(session):
    print("\nInserindo um novo usuário")
    nome = input("Nome: ")
    sobrenome = input("Sobrenome: ")
    telefone = input("Telefone: ")
    email = input("Email: ")
    cpf = input("CPF: ")

    print("\nEndereço:")
    rua = input("Rua: ")
    num = input("Número: ")
    bairro = input("Bairro: ")
    cidade = input("Cidade: ")
    estado = input("Estado: ")
    cep = input("CEP: ")

    end = [{
        "rua": rua,
        "num": num,
        "bairro": bairro,
        "cidade": cidade,
        "estado": estado,
        "cep": cep
    }]

    query = SimpleStatement("""
    INSERT INTO usuario (nome, sobrenome, cpf, telefone, email, end, favorito) 
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """)
    session.execute(query, (nome, sobrenome, cpf, telefone, email, end, []))
    print(f"Usuário {nome} {sobrenome} inserido com sucesso")

def read_usuario(session, cpfUsuario=None):
    if cpfUsuario:
        query = SimpleStatement("SELECT * FROM usuario WHERE cpf=%s", consistency_level=ConsistencyLevel.LOCAL_QUORUM)
        users = session.execute(query, (cpfUsuario,))
        user = users.one()
        if user:
            print_user_info(session, user)
        else:
            print(f"Usuário com CPF '{cpfUsuario}' não encontrado.")
    else:
        query = SimpleStatement("SELECT * FROM usuario", consistency_level=ConsistencyLevel.LOCAL_QUORUM)
        users = session.execute(query)
        for user in users:
            print_user_info(session, user)


def update_usuario(session, cpf_usuario):
    query = SimpleStatement("SELECT * FROM usuario WHERE cpf=%s", consistency_level=ConsistencyLevel.LOCAL_QUORUM)
    mydoc = session.execute(query, (cpf_usuario,)).one()

    if mydoc:
        update_fields = {}  

        print("Dados do usuário antes da atualização:")
        print_user_info(session, mydoc)

        print("\nMenu de opções:")
        print("1 - Mudar Nome")
        print("2 - Mudar Sobrenome")
        print("3 - Mudar Telefone")
        print("4 - Mudar Email")
        print("5 - Mudar Endereço")
        print("6 - Voltar ao menu principal")

        while True:
            opcao = input("\nEscolha uma opção: ")

            if opcao == "1":
                nome = input("Novo Nome: ")
                if nome:
                    update_fields["nome"] = nome
            elif opcao == "2":
                sobrenome = input("Novo Sobrenome: ")
                if sobrenome:
                    update_fields["sobrenome"] = sobrenome
            elif opcao == "3":
                telefone = input("Novo Telefone: ")
                if telefone:
                    update_fields["telefone"] = telefone
            elif opcao == "4":
                email = input("Novo Email: ")
                if email:
                    update_fields["email"] = email
            elif opcao == "5":
                print("\nEndereço atual:")
                for endereco in mydoc.end:
                    print("Rua:", endereco["rua"])
                    print("Número:", endereco["num"])
                    print("Bairro:", endereco["bairro"])
                    print("Cidade:", endereco["cidade"])
                    print("Estado:", endereco["estado"])
                    print("CEP:", endereco["cep"])
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
                update_fields["end"] = [endereco]
            elif opcao == "6":
                print("Retornando ao menu principal...")
                break
            else:
                print("Opção inválida. Por favor, escolha uma opção válida.")

        update_query = "UPDATE usuario SET nome=%s, sobrenome=%s, telefone=%s, email=%s, end=%s WHERE cpf=%s"
        session.execute(update_query, (
            update_fields.get("nome", mydoc.nome),
            update_fields.get("sobrenome", mydoc.sobrenome),
            update_fields.get("telefone", mydoc.telefone),
            update_fields.get("email", mydoc.email),
            update_fields.get("end", mydoc.end),
            cpf_usuario
        ))
        print("Usuário atualizado com sucesso.")

    else:
        print(f"Usuário com CPF '{cpf_usuario}' não encontrado.")



def visualizar_favoritos(session, cpf_usuario):
    query = SimpleStatement("""
    SELECT * FROM favoritos WHERE cpf_usuario=%s ALLOW FILTERING
    """)
    mydoc = session.execute(query, (cpf_usuario,))
    for favorito in mydoc:
        produto = session.execute("SELECT * FROM produto WHERE id=%s", (favorito["id_produto"],)).one()
        if produto:
            vendedor = session.execute("SELECT * FROM vendedor WHERE cpf=%s", (produto["vendedor"],)).one()
            if vendedor:
                print("Nome do Produto:", produto["nome"])
                print("Preço:", produto["preco"])
                print("Vendedor:", vendedor["nome"])
                print()
            else:
                print("Vendedor não encontrado para o produto:", produto["nome"])
        else:
            print("Produto não encontrado para o favorito com ID:", favorito["id"])

def ver_compras_realizadas(session, cpf_usuario):
    query = SimpleStatement("""
    SELECT * FROM compras WHERE cpf_usuario=%s ALLOW FILTERING
    """)
    compras_realizadas = session.execute(query, (cpf_usuario,))
    count = 0

    for compra in compras_realizadas:
        count += 1
        print(f"ID da Compra: {compra['id']}")
        print("Produtos:")
        for produto in compra['produtos']:
            print(f"   Nome do Produto: {produto['nome']} | Preço: {produto['preco']}")
        print(f"Endereço de Entrega: {compra['endereco_entrega']}")
        print("----")

    if count == 0:
        print("Nenhuma compra encontrada para este usuário.")



def print_user_info(session, user):
    print("Nome:", user.nome)
    print("Sobrenome:", user.sobrenome)
    print("CPF:", user.cpf)
    print("Telefone:", user.telefone)
    print("Email:", user.email)
    print("Endereços:")
    for endereco in user.end:
        print("Rua:", endereco["rua"])
        print("Número:", endereco["num"])
        print("Bairro:", endereco["bairro"])
        print("Cidade:", endereco["cidade"])
        print("Estado:", endereco["estado"])
        print("CEP:", endereco["cep"])
    print("Favoritos:")
    visualizar_favoritos(session, user.cpf)
    print("Compras Realizadas:")
    ver_compras_realizadas(session, user.cpf)
    print("----")


def test_crud_operations():
    from connect_database import create_session

    session = create_session()
    create_tables(session)

    # Teste de criação de usuário
    print("\nTeste de criação de usuário:")
    create_usuario(session)

    # Teste de leitura de usuário
    print("\nTeste de leitura de usuário:")
    nomeUsuario = input("Digite o nome do usuário para buscar: ")
    read_usuario(session, nomeUsuario)

    # Teste de atualização de usuário
    print("\nTeste de atualização de usuário:")
    cpf_usuario = input("Digite o CPF do usuário para atualizar: ")
    update_fields = {
        "telefone": "987654321",
        "email": "john.doe@newdomain.com"
    }
    update_usuario(session, cpf_usuario, update_fields)

    # Teste de exclusão de usuário
    print("\nTeste de exclusão de usuário:")
    cpf_usuario = input("Digite o CPF do usuário para deletar: ")
    delete_usuario(session, cpf_usuario)

if __name__ == "__main__":
    test_crud_operations()
