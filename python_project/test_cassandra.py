import connect_database
import cassandra_operations

def test_cassandra_operations():
    session = connect_database.create_session()
    cassandra_operations.create_tables(session)
    
    # Criação de um novo usuário
    nome = "John"
    sobrenome = "Doe"
    telefone = "123456789"
    email = "john.doe@example.com"
    cpf = "12345678900"
    end = [{"rua": "Main St", "num": "123", "bairro": "Downtown", "cidade": "Metropolis", "estado": "MT", "cep": "12345"}]
    cassandra_operations.create_usuario(session, nome, sobrenome, telefone, email, cpf, end)
    
    # Leitura de usuário
    cassandra_operations.read_usuario(session, "John")
    
    # Atualização de usuário
    update_fields = {
        "telefone": "987654321",
        "email": "john.doe@newdomain.com"
    }
    cassandra_operations.update_usuario(session, "John", update_fields)
    
    # Leitura de usuário após atualização
    cassandra_operations.read_usuario(session, "John")
    
    # Deletar usuário pelo CPF
    cassandra_operations.delete_usuario(session, cpf)

if __name__ == "__main__":
    test_cassandra_operations()
