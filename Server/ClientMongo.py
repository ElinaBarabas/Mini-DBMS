import json
import os
from pymongo import MongoClient
from bson.objectid import ObjectId


class ClientMongo:
    def __init__(self):
        host = 'localhost'
        port = 27017
        self.client = MongoClient(host, port)

    def update_mongoDB(self):  # always check if new files were created and updates the DB
        # Directory where your JSON files are located
        # json_directory = 'C:\\Users\\user\\Desktop\\Mini dbsm\\json'
        json_directory = 'C:\\Users\\User\\Desktop\\Mini-DBMS\\json'
        json_files = [f for f in os.listdir(json_directory) if f.endswith('.json')]
        # Loop through JSON files in the directory
        for json_file in json_files:
            db_name = json_file.replace('.json', '')
            db = self.client[db_name]
            db_name = db_name.upper()

            with open(os.path.join(json_directory, json_file), 'r') as file:
                data = json.load(file)

            values = data.get(db_name, {})
            values = values.get('Tables', {})

            for value in values:
                collection_name = value
                if collection_name in db.list_collection_names():  # Check if the collection exists
                    print(f"Table '{collection_name}' already exists.")
                else:  # If the collection does not exist, create it
                    db.create_collection(collection_name)
                    print(f"Table '{collection_name}' created successfully.")

    def insert_mongoDB(self, database_name, table_name):

        file_directory = os.getcwd()
        file_directory = os.path.abspath(os.path.join(file_directory, os.pardir))
        file_directory += f"\\json\\"

        current_directory = file_directory
        existing_files = os.listdir(current_directory)

        file_name = f"{database_name}.json"

        database_exists = 0

        for file in existing_files:
            if file == file_name:
                database_exists = 1
        if database_exists == 0:
            raise Exception(f"There is no database with this name {database_name}")
        file_path = os.path.join(file_directory, file_name)

        if not os.path.isfile(file_path):
            print(f"Error: File '{file_path}' does not exist.")
            return
        try:
            with open(file_path, 'r') as json_file:
                data = json.load(json_file)
            if not (data[database_name.upper()]["Tables"][table_name]):
                print(f"There is no such table {table_name}")
            if "PK" in data[database_name.upper()]["Tables"][table_name]["Keys"]:
                pk = data[database_name.upper()]["Tables"][table_name]["Keys"]["PK"]
                print("pk", pk)
            if "Attributes" in data[database_name.upper()]["Tables"][table_name]:
                attributes = data[database_name.upper()]["Tables"][table_name]["Attributes"]
                print("attributes", attributes)
        except Exception as e:
            print(f"An error occurred: {str(e)}")

    def add_mongoDB(self, id, attributes, database_name, table_name):  # adds data as _id and attributes-string in a collection
        db = self.client[database_name]
        collection = db[table_name]

        # Check if a document with the specified _id already exists
        existing_doc = collection.find_one({"_id": id})

        if existing_doc is not None:
            # Document with the specified _id already exists
            print(f"Document with _id {id} already exists")
        else:
            # Document with the specified _id does not exist, so you can insert it
            data = {
                "_id": id,
                "Value": attributes
            }
            collection.insert_one(data)
            print(f"Data inserted with custom _id: {id}")

    def delete_mongoDB(self, id, database_name, table_name):  # deletes data based on _id  in a collection
        db = self.client[database_name]
        collection = db[table_name]

        # Delete the document based on the  _id
        delete_result = collection.delete_one({"_id": id})

        if delete_result.deleted_count == 1:
            print(f"Document with _id '{id}' deleted successfully.")
        else:
            raise Exception(f"No document found with _id '{id}'.")

    def drop_database(self, database_name):  # deletes the DB, used when deleting a json file DB
        self.client.drop_database(database_name)

    def drop_table(self, database_name, table_name):  # deletes the collection, used when deleting  a table
        db = self.client[database_name]
        collection = db[table_name]
        collection.drop()

    def close_mongoDB(self):
        self.client.close()



