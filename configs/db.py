import os
from dotenv import load_dotenv
from elasticsearch import Elasticsearch

load_dotenv()

# Elasticsearch
# Define Elasticsearch configuration
es_config = {
    "host": os.getenv("ES_HOST", "localhost"),
    "port": int(os.getenv("ES_PORT", "9200")),  # Convert port to integer
    "api_version": os.getenv("ES_VERSION", "7.17.7"),
    "timeout": 60 * 60,
    "use_ssl": False
}

# Check if authentication variables are defined in .env
es_user = os.getenv("ES_USER")
es_password = os.getenv("ES_PASSWORD")

# Add HTTP authentication to the configuration if username and password are provided
if es_user and es_password:
    es_config["http_auth"] = (es_user, es_password)

# Create an Elasticsearch client instance
client = Elasticsearch(**es_config)
