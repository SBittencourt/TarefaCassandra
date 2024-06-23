from astrapy import DataAPIClient

# Initialize the client

client = DataAPIClient("AstraCS:ytnWEwZaMJqFMYCvfwAPAYTl:79ce3b83a7d38b3cfd13302a23c4db9b52fc8757cbd010eae03a4da6195a06ec")
db = client.get_database_by_api_endpoint(
  "https://26d2f58b-da91-4410-bb44-ab1f51f90876-us-east-2.apps.astra.datastax.com"
)

print(f"Connected to Astra DB: {db.list_collection_names()}")

