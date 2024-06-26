from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json

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
