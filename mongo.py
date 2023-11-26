from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['amalia']  # Replace 'your_database' with your database name
collection = db['ioana']  # Replace 'your_collection' with your collection name

# Iterate over all documents in the collection
for document in collection.find():
    doc_id = document['_id']  # Accessing the _id field
    other_value = document['Value']  # Replace 'field_name' with your field name
    print(f"Document ID: {doc_id}, Value: {other_value}")

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
            if table_name in tables:
                attributes = tables[table_name]['Attributes']
                # Check if the specified attribute exists in the table
                if attribute_name in attributes:
                    # Check if the attribute is part of any index
                    for index_name, index_definition in indexes_dict.items():
                        table_and_attributes = index_definition.strip("()").split(", ")
                        if table_name in table_and_attributes[0]:
                            position = table_and_attributes.index(attribute_name) - 1
                            return {position + 1}

        return 0