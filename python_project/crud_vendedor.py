from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel

def delete_vendedor(session, cpf):
    query = SimpleStatement("DELETE FROM vendedor WHERE cpf = %s")
    session.execute(query, (cpf,))
    print(f"Deletado o vendedor com CPF {cpf}")

def create_tables(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS vendedor (
        nome text,
        cpf text PRIMARY KEY,
        telefone text,
        email text,
        enderecos list<frozen<map<text, text>>>
    )
    """)

def create_vendedor(session):
    print("\nInserindo um novo vendedor")
    nome = input("Nome: ")
    cpf = input("CPF: ")
    telefone = input("Telefone: ")
    email = input("Email: ")

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
    enderecos = [endereco]

    query = SimpleStatement("""
    INSERT INTO vendedor (nome, cpf, telefone, email, enderecos) 
    VALUES (%s, %s, %s, %s, %s)
    """)
    session.execute(query, (nome, cpf, telefone, email, enderecos))
    print(f"Vendedor {nome} inserido com sucesso")

def read_vendedor(session, cpf_vendedor=None):
    print("Informações do vendedor:")

    if cpf_vendedor:
        query = SimpleStatement("SELECT * FROM vendedor WHERE cpf=%s", consistency_level=ConsistencyLevel.LOCAL_QUORUM)
        vendedores = session.execute(query, (cpf_vendedor,))
        vendedor = vendedores.one()
        if vendedor:
            print_vendedor_info(vendedor) 
        else:
            print(f"Vendedor com CPF '{cpf_vendedor}' não encontrado.")
    else:
        query = SimpleStatement("SELECT * FROM vendedor", consistency_level=ConsistencyLevel.LOCAL_QUORUM)
        vendedores = session.execute(query)
        for vendedor in vendedores:
            print_vendedor_info(vendedor)  
  



def print_vendedor_info(vendedor):
    print("Nome:", vendedor.nome)
    print("CPF:", vendedor.cpf)
    print("Telefone:", vendedor.telefone)
    print("Email:", vendedor.email)
    print("Endereços:")
    for endereco in vendedor.enderecos:
        print("Rua:", endereco["rua"])
        print("Número:", endereco["num"])
        print("Bairro:", endereco["bairro"])
        print("Cidade:", endereco["cidade"])
        print("Estado:", endereco["estado"])
        print("CEP:", endereco["cep"])
    print("----")


def update_vendedor(session, cpf_vendedor, update_fields):
    query = SimpleStatement("SELECT * FROM vendedor WHERE cpf=%s", consistency_level=ConsistencyLevel.LOCAL_QUORUM)
    vendedor = session.execute(query, (cpf_vendedor,)).one()

    if vendedor:
        print("Dados do vendedor antes da atualização:")
        print_vendedor_info(vendedor) 

        print("\nMenu de opções:")
        print("1 - Mudar Nome")
        print("2 - Mudar Telefone")
        print("3 - Mudar Email")
        print("4 - Mudar Endereço")
        print("5 - Voltar ao menu principal")

        while True:
            opcao = input("\nEscolha uma opção: ")

            if opcao == "1":
                novo_nome = input("Novo Nome: ")
                if novo_nome:
                    update_fields["nome"] = novo_nome
            elif opcao == "2":
                novo_telefone = input("Novo Telefone: ")
                if novo_telefone:
                    update_fields["telefone"] = novo_telefone
            elif opcao == "3":
                novo_email = input("Novo Email: ")
                if novo_email:
                    update_fields["email"] = novo_email
            elif opcao == "4":
                print("\nEndereço atual:")
                for endereco in vendedor.enderecos:
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
                update_fields["enderecos"] = [endereco]
            elif opcao == "5":
                print("Retornando ao menu principal...")
                break
            else:
                print("Opção inválida. Por favor, escolha uma opção válida.")

        update_query = "UPDATE vendedor SET nome=%s, telefone=%s, email=%s, enderecos=%s WHERE cpf=%s"
        session.execute(update_query, (
            update_fields.get("nome", vendedor.nome),
            update_fields.get("telefone", vendedor.telefone),
            update_fields.get("email", vendedor.email),
            update_fields.get("enderecos", vendedor.enderecos),
            cpf_vendedor
        ))
        print("Vendedor atualizado com sucesso.")

    else:
        print(f"Vendedor com CPF '{cpf_vendedor}' não encontrado.")


