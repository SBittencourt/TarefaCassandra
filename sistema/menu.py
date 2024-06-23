import crud_usuario

key = 0
sub = 0
while key != 'S':
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
            crud_usuario.create_usuario()
        elif sub == '2':
            nomeUsuario = input("Visualizar usuário, deseja algum nome especifico? ")
            crud_usuario.read_usuario(nomeUsuario)
        elif sub == '3':
            nomeUsuario = input("Atualizar usuário, deseja algum nome especifico? ")
            crud_usuario.update_usuario(nomeUsuario)
        elif sub == '4':
            nomeUsuario = input("Nome do usuário a ser deletado: ")
            sobrenome = input("Sobrenome do usuário a ser deletado: ")
            crud_usuario.delete_usuario(nomeUsuario, sobrenome)
    
    
    elif key == '2':
        print("Menu do Vendedor")
        print("1 - Criar Vendedor")
        print("2 - Visualizar Vendedor")
        print("3 - Atualizar Vendedor")
        print("4 - Deletar Vendedor")
        sub = input("Digite a opção desejada? (V para voltar) ")
    
        if sub == '1':
            print("Criar Vendedor")
            crud_vendedor.create_vendedor()
        
        elif sub == '2':
            nomeVendedor = input("Ler vendedor, deseja algum nome especifico? ")
            crud_vendedor.read_vendedor(nomeVendedor)
        
        elif sub == '3':
            nomeVendedor = input("Atualizar vendedor, deseja algum nome especifico? ")
            crud_vendedor.update_vendedor(nomeVendedor)

        elif sub == '4':
            print("Deletar Vendedor")
            nomeVendedor = input("Nome do vendedor a ser deletado: ")
            cpfVendedor = input("CPF do vendedor a ser deletado: ")
            crud_vendedor.delete_vendedor(nomeVendedor, cpfVendedor)


    elif key == '3':
        print("Menu do Produto") 
        print("1 - Criar Produto")
        print("2 - Ver Produto")
        print("3 - Atualizar Produto")
        print("4 - Deletar Produto")
        sub = input("Digite a opção desejada? (V para voltar) ")
        if sub == '1':
            print("Criar Produto")
            crud_produto.create_produto()
            
        elif sub == '2':
            nomeProduto = input("Visualizar produtos, deseja algum nome especifico? Caso não, pressione enter")
            crud_produto.read_produto(nomeProduto)
        
        elif sub == '3':
            nomeProduto = input("Atualizar produtos, deseja algum nome especifico? ")
            crud_produto.update_produto(nomeProduto)

        elif sub == '4':
            print("deletar produto")
            nomeProduto = input("Nome a ser deletado: ")
            crud_produto.delete_produto(nomeProduto)


    elif key == '4':
        print("Compras") 
        print("1 - Realizar compra")
        print("2 - Ver compras realizadas")
        sub = input("Digite a opção desejada? (V para voltar) ")

        if sub == '1':
            cpf_usuario = input("Digite o CPF do usuário: ")
            carrinho_usuario = crud_compras.realizar_compra(cpf_usuario)
              
        elif sub == '2':
            cpf_usuario = input("Digite o CPF do usuário: ")
            crud_compras.ver_compras_realizadas(cpf_usuario)
        else:
            print("Opção inválida. Por favor, digite uma opção válida.")


    elif key == '5':
            print("Favoritos") 
            print("1 - Adicionar favoritos")
            print("2 - Visualizar favoritos")
            print("3 - Deletar favoritos")
            sub = input("Digite a opção desejada? (V para voltar) ")

            if sub == '1':
                crud_favoritos.adicionarnovo_favorito()

            elif sub == '2':
                cpf_usuario = input("Digite o CPF do usuário: ")
                crud_favoritos.visualizar_favoritos(cpf_usuario)

            elif sub == '3':
                cpf_usuario = input("Digite o CPF do usuário: ")
                id_produto = input("Digite o ID do produto que deseja remover dos favoritos: ")
                crud_favoritos.excluir_favorito(cpf_usuario, id_produto)

            else:
                print("Opção inválida. Por favor, digite uma opção válida.")

print("Tchau, tchau! Volte sempre!")
