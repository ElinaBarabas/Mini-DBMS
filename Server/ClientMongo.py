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

            table_values = data.get(db_name, {})
            table_values = table_values.get('Tables', {})

#check if there are new tables added and update the DB

            for t_value in table_values:
                table_name = t_value
                if table_name not in db.list_collection_names():  # Check if the collection exists # If the collection does not exist, create it
                    db.create_collection(table_name)
                    print(f"Table '{table_name}' created successfully.")
#create UNIQUE indexes if nedded
            index_values = data.get(db_name, {}).get('Indexes', {}).get('Unique', {})
            for i_value,table in index_values.items():

                table = table.strip("()").split(",")
                collection_name = "INDEX_" + table[0] + "_" + i_value
                if collection_name not in db.list_collection_names():  # Check if the collection exists
                    db.create_collection(collection_name)
                    print(f"Index '{collection_name}' created successfully.")

# DO THE SAME FOR fk NON UNIQUE INDEXES
            index_values = data.get(db_name, {}).get('Indexes', {}).get('NonUnique', {})
            for i_value,table in index_values.items():

                table = table.strip("()").split(",")
                collection_name = "INDEX_" + table[0] + "_" + i_value
                if collection_name not in db.list_collection_names():   # If the collection does not exist, create it
                    db.create_collection(collection_name)
                    print(f"Index '{collection_name}' created successfully.")
#check if there are deleted tables (not in folder but in mongoDB exist) and delete them
            mongo_collections = db.list_collection_names()
            collections_to_delete = [coll for coll in mongo_collections if coll not in table_values and "INDEX_" not in coll]
            for collection_name in collections_to_delete:
                print("collections_to_delete:", collections_to_delete)
                db[collection_name].drop()
# check if tables were deleted and delete the afferent indexes for them
                index_name = "INDEX_" + collection_name
                indexes_to_delete = [coll for coll in mongo_collections if coll not in table_values and index_name in coll]
                for index_name in indexes_to_delete:
                    print("indexes_to_delete:", indexes_to_delete)
                    db[index_name].drop()



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

        unique, non_unique = self.return_indexes_from_json(database_name,table_name)
        # Check if a document with the specified _id already exists
        existing_doc = collection.find_one({"_id": id})
#check if the speified id alredy exists
        if existing_doc is not None:

            # Document with the specified _id already exists
            raise Exception (f"Document with _id {id} already exists")
        elif not unique:
            try:
# Document with the specified _id does not exist and there are no unique indexes so you can insert it without checking the unicity
                data = {
                    "_id": id,
                    "Value": attributes
                    }
                collection.insert_one(data)
                print(f"Data inserted with custom _id: {id}")
    # function to add in index non-unique file too. NOT WORKING PROPERLY
                if non_unique:
                    value2=""
                    attributes = attributes.split("#") #to add 1#2#4
                    for key,value in non_unique.items():
                        print(key, value)
                        n_index = value.strip("()").split(",")[1:]
                        for n in n_index:
                            print("n in n_index: ",n)
                            n ="".join(n.split())
                            position = self.return_attribute_position(database_name, table_name, non_unique, n)
                            print("POS",position)
                            print("attributes: ",attributes)
                            value2 = attributes[position-2]
                        collection_index_name = "INDEX_"+ table_name + "_" + key
                        collection_index = db[collection_index_name]
                        print("collection_index: ",collection_index_name)
                        index_doc = collection_index.find_one({"_id": id})
                        print("value22:",value2)
                        if index_doc is None:
                            data_index = {
                                "_id": value2,
                                "Value": concatenated_id
                            }
                        else:
                            print("idc",index_doc['Value'])
                            concatenated_id = str(index_doc['Value']) + "#" + str(id)
                            data_index = {
                                "_id": value2,
                                "Value": concatenated_id
                            }
                        collection_index.insert_one(data_index)
                        print(f"Data inserted with custom _id for INDEX: {value2}")
            except Exception as e:(" Exception for non unique indexes")
        else: #it means that there are unique indexes so we need to check it. TO BE COTINUED, NOT COMPLETED
            att_name = "y"
            result = self.return_attribute_position(database_name, table_name, unique, att_name)


            for document in collection.find():
                doc_id = document['_id']  # Accessing the _id field
                other_value = document['Value']  # Accessing the Values concatenated
                print(f"Document ID: {doc_id}, Value: {other_value}")
            att_name = "y"
            result = self.return_attribute_position(database_name, table_name, unique, att_name)




        #else:
            # Document with the specified _id does not exist, so you can insert it
         #   data = {
          #      "_id": id,
           #     "Value": attributes
            #}
            #collection.insert_one(data)
            #print(f"Data inserted with custom _id: {id}")

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

    def return_indexes_from_json(self, database_name,table_name):
        file_directory = os.getcwd()
        file_directory = os.path.abspath(os.path.join(file_directory, os.pardir))
        file_directory += f"\\json\\"

        file_name = f"{database_name.lower()}.json"

        current_directory = file_directory
        existing_files = os.listdir(current_directory)

        database_exists = 0

        for file in existing_files:
            if file == file_name:
                database_exists = 1

        if database_exists == 0:
            raise Exception(f"There is no database with this name {database_name}")

        file_path = os.path.join(file_directory, file_name)

        database_name = database_name.upper()

        with open(file_path, 'r') as json_file:
            json_data = json.load(json_file)

        unique_indexes = {}
        non_unique_indexes = {}

        for database_name in json_data.values():
            for index_type,indexes in database_name['Indexes'].items():
                for index_name,index_definition in indexes.items():
                    if index_type == 'Unique':
                        unique_indexes[index_name] = index_definition
                    else:
                        non_unique_indexes[index_name] = index_definition
        unique_indexes = {k: v for k, v in unique_indexes.items() if v.startswith(table_name)}
        print(f"UNIQUE INDEXES FOR TABLE {table_name} are ", unique_indexes)
        non_unique_indexes = {k: v for k, v in non_unique_indexes.items() if v.startswith("(" + table_name)}
        print(f"NON UNIQUE INDEXES FOR TABLE {table_name} are ", non_unique_indexes)

        return unique_indexes, non_unique_indexes


    def return_attribute_position(self,database_name,table_name,indexes_dict,attribute_name):
        attribute_name = attribute_name.replace(" ", "")
        file_directory = os.getcwd()
        file_directory = os.path.abspath(os.path.join(file_directory, os.pardir))
        file_directory += f"\\json\\"

        file_name = f"{database_name.lower()}.json"

        current_directory = file_directory
        existing_files = os.listdir(current_directory)

        database_exists = 0

        for file in existing_files:
            if file == file_name:
                database_exists = 1

        if database_exists == 0:
            raise Exception(f"There is no database with this name {database_name}")

        file_path = os.path.join(file_directory, file_name)

        database_name = database_name.upper()

        with open(file_path, 'r') as json_file:
            json_data = json.load(json_file)

#get the data from a dict

        if database_name in json_data:
            tables = json_data[database_name]['Tables']
            # Check if the specified table exists in the database
            print("TABLES", tables)
            if table_name in tables:
                attributes = tables[table_name]['Attributes']
                print("ATTRIBUTES", attributes)
                print("ATtribute NAME",attribute_name)
                position = None
                for idx, key in enumerate(attributes, start = 1):
                    if key == attribute_name:
                        position = idx


        return position
