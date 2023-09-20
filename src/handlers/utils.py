from pymongo import MongoClient


def connect_db(database: str):
    try:
        uri = "mongodb://localhost:27017/bd"
        client = MongoClient(uri)
        db = client[database]
        print("Connection Successful")
        return db
    except Exception as ex:
        print("Fallo estableciendo la conexión a la bd")
        print(ex)


def close_connect_db(client):
    try:
        client.close()
    except Exception as ex:
        print("Error cerrando la conexión a la bd")
        print(ex)
