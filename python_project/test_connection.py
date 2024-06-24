import connect_database

def test_connection():
    try:
        session = connect_database.create_session()
        release_version = connect_database.get_release_version(session)
        print(f"Cassandra Release Version: {release_version}")
        return True
    except Exception as e:
        print(f"Connection test failed: {e}")
        return False

if __name__ == "__main__":
    if test_connection():
        print("Test passed.")
    else:
        print("Test failed.")
