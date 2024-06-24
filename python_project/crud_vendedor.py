from cassandra.cluster import Cluster, ConsistencyLevel
from cassandra.query import SimpleStatement
import connect_database  # Importe o módulo connect_database

# Função para deletar vendedor
# Função para deletar vendedor
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

    query = SimpleStatement("""
    INSERT INTO vendedor (nome, cpf, telefone, email, enderecos) 
    VALUES (%s, %s, %s, %s, %s)
    """)
    session.execute(query, (nome, cpf, telefone, email, enderecos))
    print(f"Vendedor {nome} inserido com sucesso")

def read_vendedor(session, nome_vendedor=None):
    print("Informações do vendedor:")
    if nome_vendedor is None:
        query = SimpleStatement("SELECT * FROM vendedor")
        vendedores = session.execute(query)
        for vendedor in vendedores:
            print_vendedor_info(session, vendedor)
    else:
        query = SimpleStatement("SELECT * FROM vendedor WHERE nome=%s ALLOW FILTERING")
        vendedores = session.execute(query, (nome_vendedor,))
        for vendedor in vendedores:
            print_vendedor_info(session, vendedor)

def print_vendedor_info(session, vendedor):
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

def update_vendedor(session, nome_vendedor, cpf_vendedor, update_fields):
    query = SimpleStatement("SELECT * FROM vendedor WHERE nome=%s AND cpf=%s ALLOW FILTERING", consistency_level=ConsistencyLevel.LOCAL_QUORUM)
    vendedor = session.execute(query, (nome_vendedor, cpf_vendedor)).one()

    if vendedor:
        print("Dados do vendedor antes da atualização:")
        print_vendedor_info(session, vendedor)

        print("\nMenu de opções:")
        print("1 - Mudar Nome")
        print("2 - Mudar CPF")
        print("3 - Mudar Telefone")
        print("4 - Mudar Email")
        print("5 - Mudar Endereço")
        print("6 - Voltar ao menu principal")

        while True:
            opcao = input("\nEscolha uma opção: ")

            if opcao == "1":
                novo_nome = input("Novo Nome: ")
                if novo_nome:
                    update_fields["nome"] = novo_nome
            elif opcao == "2":
                novo_cpf = input("Novo CPF: ")
                if novo_cpf:
                    update_fields["cpf"] = novo_cpf
            elif opcao == "3":
                novo_telefone = input("Novo Telefone: ")
                if novo_telefone:
                    update_fields["telefone"] = novo_telefone
            elif opcao == "4":
                novo_email = input("Novo Email: ")
                if novo_email:
                    update_fields["email"] = novo_email
            elif opcao == "5":
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
            elif opcao == "6":
                print("Retornando ao menu principal...")
                break
            else:
                print("Opção inválida. Por favor, escolha uma opção válida.")

        # Preparando a atualização
        update_query = "UPDATE vendedor SET nome=%s, cpf=%s, telefone=%s, email=%s, enderecos=%s WHERE nome=%s AND cpf=%s"
        session.execute(update_query, (
            update_fields.get("nome", vendedor.nome),
            update_fields.get("cpf", vendedor.cpf),
            update_fields.get("telefone", vendedor.telefone),
            update_fields.get("email", vendedor.email),
            update_fields.get("enderecos", vendedor.enderecos),
            nome_vendedor,
            cpf_vendedor
        ))
        print("Vendedor atualizado com sucesso.")

    else:
        print(f"Vendedor com nome '{nome_vendedor}' e CPF '{cpf_vendedor}' não encontrado.")

def test_crud_operations():
    session = connect_database.create_session()  # Chama create_session() do módulo connect_database
    create_tables(session)
    
    # Teste de criação de vendedor
    print("\nTeste de criação de vendedor:")
    create_vendedor(session)
    
    # Teste de leitura de vendedor
    print("\nTeste de leitura de vendedor:")
    read_vendedor(session)
    
    # Teste de atualização de vendedor
    print("\nTeste de atualização de vendedor:")
    update_fields = {
        "telefone": "987654321",
        "email": "jane.doe@newdomain.com"
    }
    update_vendedor(session, "João Silva", "12345678900", update_fields)
    
    # Teste de exclusão de vendedor
    print("\nTeste de exclusão de vendedor:")
    delete_vendedor(session, "12345678900")

if __name__ == "__main__":
    test_crud_operations()
