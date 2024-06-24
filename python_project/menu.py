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
        print("5 - Favoritos")
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
                nomeUsuario = input("Visualizar usuário, deseja algum nome específico? ")
                crud_usuario.read_usuario(session, nomeUsuario)

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
                cpfVendedor = input("Ler vendedor, digite o CPF do vendedor: ")
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
            print("Menu do Produto")
            print("1 - Criar Produto")
            print("2 - Ver Produto")
            print("3 - Atualizar Produto")
            print("4 - Deletar Produto")
            sub = input("Digite a opção desejada? (V para voltar) ")

            if sub == '1':
                print("Criar Produto")
                crud_produto.create_produto(session)

            elif sub == '2':
                nomeProduto = input("Ver produtos, deseja algum nome específico? Pressione Enter para ver todos")
                crud_produto.read_produto(session, nomeProduto)

            elif sub == '3':
                nomeProduto = input("Atualizar produtos, deseja algum nome específico? ")
                crud_produto.update_produto(session, nomeProduto)

            elif sub == '4':
                print("Deletar Produto")
                nomeProduto = input("Nome do produto a ser deletado: ")
                crud_produto.delete_produto(session, nomeProduto)

        elif key == '4':  # Compras
            print("Menu de Compras")
            print("1 - Realizar Compra")
            print("2 - Ver Compras Realizadas")
            sub = input("Digite a opção desejada? (V para voltar) ")

            if sub == '1':
                cpf_usuario = input("Digite o CPF do usuário: ")
                carrinho_usuario = crud_compras.realizar_compra(session, cpf_usuario)

            elif sub == '2':
                cpf_usuario = input("Digite o CPF do usuário: ")
                crud_compras.ver_compras_realizadas(session, cpf_usuario)

        elif key == '5':  # Favoritos
            print("Menu de Favoritos")
            print("1 - Adicionar Favorito")
            print("2 - Visualizar Favoritos")
            print("3 - Remover Favorito")
            sub = input("Digite a opção desejada? (V para voltar) ")

            if sub == '1':
                crud_favoritos.adicionar_favorito(session)

            elif sub == '2':
                cpf_usuario = input("Digite o CPF do usuário: ")
                crud_favoritos.visualizar_favoritos(session, cpf_usuario)

            elif sub == '3':
                cpf_usuario = input("Digite o CPF do usuário: ")
                id_produto = input("Digite o ID do produto a ser removido dos favoritos: ")
                crud_favoritos.remover_favorito(session, cpf_usuario, id_produto)

        else:
            print("Opção inválida. Por favor, digite uma opção válida.")

    session.shutdown()  # Encerrando a sessão do Cassandra ao sair do menu
    print("Tchau, tchau! Volte sempre!")

if __name__ == "__main__":
    main_menu()
