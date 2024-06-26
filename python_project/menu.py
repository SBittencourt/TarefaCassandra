from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json

import crud_usuario
import crud_vendedor
import crud_produto
import crud_compras
import crud_favoritos

def create_session():
    cloud_config = {
        'secure_connect_bundle': 'secure-connect-mercadolivre.zip'
    }

    with open("mercadolivre.com-token.json") as f:
        secrets = json.load(f)

    CLIENT_ID = secrets["clientId"]
    CLIENT_SECRET = secrets["secret"]

    auth_provider = PlainTextAuthProvider(CLIENT_ID, CLIENT_SECRET)
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()

    session.execute("USE mercadolivre")
    
    return session

def main_menu():
    session = create_session() 
    key = '0'
    sub = '0'

    while key.upper() != 'S':
        print("1 - CRUD Usuário")
        print("2 - CRUD Vendedor")
        print("3 - CRUD Produto")
        print("4 - Compras")
        key = input("Digite a opção desejada? (S para sair) ")

        if key == '1':  
            print("Menu do Usuário")
            print("1 - Criar Usuário")
            print("2 - Visualizar Usuário")
            print("3 - Atualizar Usuário")
            print("4 - Deletar Usuário")
            sub = input("Digite a opção desejada? (V para voltar) ")

            if sub == '1':
                print("Criar usuário")
                crud_usuario.create_usuario(session)

            elif sub == '2':
                cpfUsuario = input("Visualizar usuário, deseja algum cpf específico? Pressione 'enter' para ver todos ")
                crud_usuario.read_usuario(session, cpfUsuario)

            elif sub == '3':
                cpfUsuario = input("Atualizar usuário, digite o CPF do usuário: ")
                crud_usuario.update_usuario(session, cpfUsuario)

            elif sub == '4': 
                print("Deletar Usuário")
                cpf_usuario = input("CPF do usuário a ser deletado: ")
                crud_usuario.delete_usuario(session, cpf_usuario)

        elif key == '2':  
            print("Menu do Vendedor")
            print("1 - Criar Vendedor")
            print("2 - Ler Vendedor")
            print("3 - Atualizar Vendedor")
            print("4 - Deletar Vendedor")
            sub = input("Digite a opção desejada? (V para voltar) ")

            if sub == '1':
                print("Criar Vendedor")
                crud_vendedor.create_vendedor(session)

            elif sub == '2':
                cpfVendedor = input("Ler vendedor, deseja algum cpf específico? Pressione 'enter' para ver todos ")
                crud_vendedor.read_vendedor(session, cpfVendedor)

            elif sub == '3':
                cpfVendedor = input("Atualizar vendedor, digite o CPF do vendedor: ")
                update_fields = {}
                crud_vendedor.update_vendedor(session, cpfVendedor, update_fields)

            elif sub == '4':
                print("Deletar Vendedor")
                cpfVendedor = input("CPF do vendedor a ser deletado: ")
                crud_vendedor.delete_vendedor(session, cpfVendedor)

        elif key == '3':  
            print("\nMenu do Produto")
            print("1 - Criar Produto")
            print("2 - Ler Produto")
            print("3 - Atualizar Produto")
            print("4 - Deletar Produto")
            sub = input("Digite a opção desejada (V para voltar): ")

            if sub == "1":
                crud_produto.create_produto(session)

            elif sub == "2":
                cpfVendedor = input("Ler produtos, deseja algum vendedor especifico? digite o cpf, pressione 'enter' para ver todos ")
                crud_produto.read_produto(session, cpfVendedor if cpfVendedor else None)

            elif sub == "3":
                produto_id = input("Digite o ID do produto a ser atualizado: ")
                crud_produto.update_produto(session, produto_id)

            elif sub == "4":
                produto_id = input("Digite o ID do produto a ser deletado: ")
                crud_produto.delete_produto(session, produto_id)
                
            elif sub == "5" or sub.upper() == "V":
                break
            else:
                print("Opção inválida. Tente novamente.")



        elif key == '4':  
            print("\nMenu de Compras")
            print("1 - Realizar Compras")
            print("2 - Listar Compras")
            print("3 - Detalhar Compra")
            print("4 - Voltar ao Menu Principal")
            sub = input("Digite a opção desejada: ")

            if sub == '1':
                print("Realizar Compras")
                cpf_usuario = input("Digite o CPF do usuário: ")
                crud_compras.realizar_compra(session, cpf_usuario)

            elif sub == '2':
                print("Listar Compras")
                cpf_usuario = input("Digite o CPF do usuário para listar suas compras (ou pressione Enter para listar todas as compras): ").strip()
                
                if cpf_usuario:
                    crud_compras.ver_compras_realizadas(session, cpf_usuario)
                else:
                    crud_compras.ver_compras_realizadas(session, "")


            elif sub == '3':
                print("Detalhar Compra")
                compra_id = input("Digite o ID da compra para detalhar: ")
                crud_compras.detalhar_compra(session, compra_id)

            elif sub.upper() == 'V':
                print("Voltando ao Menu Principal...")
                break

            else:
                print("Opção inválida. Digite uma opção válida.")

        
if __name__ == "__main__":
    main_menu()
