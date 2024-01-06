import json
import os

from pymongo import MongoClient


class ClientMongo:
    def __init__(self):
        host = 'localhost'
        port = 27017
        self.client = MongoClient(host, port)
        self.json_directory = self.set_json_files_path()
        self.json_files = [file for file in os.listdir(self.json_directory) if file.endswith('.json')]

    @staticmethod
    def check_comparison(value1, value2, operator):
        if operator == '=':
            return value1 == value2
        elif operator == '>':
            return value1 > value2
        elif operator == '<':
            return value1 < value2

    @staticmethod
    def create_collection(database, collection_name):
        if collection_name not in database.list_collection_names():
            database.create_collection(collection_name)
            print(f"Table '{collection_name}' was created successfully.")

    @staticmethod
    def set_json_files_path():
        file_directory = os.getcwd()
        file_directory = os.path.abspath(os.path.join(file_directory, os.pardir))
        file_directory += f"\\json\\"
        return file_directory

    def check_database_existence(self, database_file):
        for current_database_file in self.json_files:
            if current_database_file == database_file:
                return True
        raise Exception(f"There is no database with this name {database_file}")

    def close_mongoDB(self):
        self.client.close()

    def update_mongoDB(self):
        for json_file in self.json_files:
            database_name = json_file.replace('.json', '')
            database = self.client[database_name]

            with open(os.path.join(self.json_directory, json_file), 'r') as file:
                data = json.load(file)
                database_data = data.get(database_name, {})
                collections_data = database_data.get('Tables', {})

                for collection_name in collections_data:
                    # create mongo collections for newly created tables
                    self.create_collection(database, collection_name)

                    # create mongo collections for FK - non-unique index table
                    fk_map = self.get_foreign_keys(database_name, collection_name)

                    for fk_name, fk_value in fk_map.items():
                        parent_fk_table = fk_value.strip("()").split(",")[0].strip()
                        parent_fk_column = fk_value.strip("()").split(",")[1].strip()

                        fk_file_name = f"FK_constraint_for_{collection_name}_on_{parent_fk_column}_from_{parent_fk_table}_INDEX"
                        self.create_collection(database, fk_file_name)

                # create mongo collections for unique index tables
                unique_index_data = data.get(database_name, {}).get('Indexes', {}).get('Unique', {})
                for index_value, table in unique_index_data.items():
                    collection_name = table.strip("()").split(",")[0]
                    index_table_name = f"{collection_name}_Unique_{index_value}_INDEX"
                    self.create_collection(database, index_table_name)
                    self.insert_old_entries_index(database_name, collection_name, index_value, index_table_name,
                                                  "Unique")

                # create mongo collections for unique index tables
                non_unique_index_data = data.get(database_name, {}).get('Indexes', {}).get('NonUnique', {})
                for index_value, table in non_unique_index_data.items():
                    collection_name = table.strip("()").split(",")[0]
                    index_table_name = f"{collection_name}_NonUnique_{index_value}_INDEX"
                    self.create_collection(database, index_table_name)
                    self.insert_old_entries_index(database_name, collection_name, index_value, index_table_name,
                                                  "NonUnique")

                # delete tables from mongoDB and its corresponding indices
                mongo_existing_collections = database.list_collection_names()
                collections_to_delete = []

                for collection in mongo_existing_collections:
                    if collection not in collections_data:
                        if "INDEX" not in collection:
                            collections_to_delete.append(collection)

                for collection in collections_to_delete:
                    print(f"Table: {collection} will be deleted")
                    database[collection].drop()

    def get_indexes_from_json(self, database_name, collection_name):
        database_file_name = f"{database_name.lower()}.json"
        if self.check_database_existence(database_file_name):
            file_path = os.path.join(self.json_directory, database_file_name)
            with open(file_path, 'r') as json_file:
                json_data = json.load(json_file)

                unique_indexes = {}
                non_unique_indexes = {}

                for database in json_data.values():
                    for index_type, indexes in database['Indexes'].items():
                        for index_name, index_definition in indexes.items():
                            if index_type == 'Unique':
                                unique_indexes[index_name] = index_definition
                            else:
                                non_unique_indexes[index_name] = index_definition

                unique_indexes = {k: v for k, v in unique_indexes.items() if v.startswith("(" + collection_name)}
                non_unique_indexes = {k: v for k, v in non_unique_indexes.items() if
                                      v.startswith("(" + collection_name)}
            return unique_indexes, non_unique_indexes

    def get_foreign_keys(self, database_name, collection_name):

        database_file_name = f"{database_name.lower()}.json"
        if self.check_database_existence(database_file_name):
            file_path = os.path.join(self.json_directory, database_file_name)
            with open(file_path, 'r') as json_file:
                json_data = json.load(json_file)

                foreign_keys = {}

                for database in json_data.values():
                    for fk_name, fk_value in database['Tables'][collection_name]["Keys"]["FK"].items():
                        foreign_keys[fk_name] = fk_value

                return foreign_keys

    def get_primary_key(self, database_name, collection_name):

        database_file_name = f"{database_name.lower()}.json"
        if self.check_database_existence(database_file_name):
            file_path = os.path.join(self.json_directory, database_file_name)
            with open(file_path, 'r') as json_file:
                json_data = json.load(json_file)

                for database in json_data.values():
                    return database["Tables"][collection_name]["Keys"]["PK"]

    def get_attribute_position(self, database_name, collection_name, attribute_name):
        attribute_name = attribute_name.replace(" ", "")
        database_file_name = f"{database_name.lower()}.json"
        position = -1
        if self.check_database_existence(database_file_name):
            file_path = os.path.join(self.json_directory, database_file_name)

            with open(file_path, 'r') as json_file:
                json_data = json.load(json_file)

                if database_name in json_data:
                    collections = json_data[database_name]['Tables']
                if collection_name in collections:
                    attributes = collections[collection_name]['Attributes']
                    for idx, key in enumerate(attributes, start=1):
                        if key == attribute_name:
                            position = idx
                            return position
        return position

    def check_insert(self, database_name, collection_name, attributes):
        database_file_name = f"{database_name.lower()}.json"

        if not self.check_database_existence(database_file_name):
            raise Exception(f"There is no such database: {database_name}")

        file_path = os.path.join(self.json_directory, database_file_name)
        if not os.path.exists(file_path):
            raise Exception(f"Database file {file_path} does not exist.")

        with open(file_path, 'r') as json_file:
            data = json.load(json_file)

        tables = data.get(database_name, {}).get("Tables", {})
        if collection_name not in tables:
            raise Exception(f"There is no such table {collection_name}")

        keys = tables[collection_name].get("Keys", {})
        if "FK" not in keys:
            return False

        fk = keys["FK"]
        collection_attributes = []

        for value in fk.values():
            item = value.strip('()')
            collection_attributes.extend(item.split(','))

        collection_attributes = [attr.strip() for attr in collection_attributes]

        if not collection_attributes:
            return True

        attribute_name = list(fk.keys())[0]
        position = self.get_attribute_position(database_name, collection_name, attribute_name)
        if position == -1:
            return False

        value_check = attributes.split("#")[position - 2]
        database = self.client[database_name.lower()]
        collection_list = database.list_collection_names()

        if collection_attributes[0] in collection_list:
            collection = database[collection_attributes[0]]

            try:  # convert the _id into an integer(if it was given as integer) or let it string
                value_check = int(value_check)
            except ValueError:
                value_check = value_check
            existing_document = collection.find_one({"_id": value_check})

            if existing_document is not None:
                return True

        raise Exception("Foreign Keys Constraints are not respected!")

    def insert_data_mongoDB(self, entity_id, attributes, database_name, collection_name):
        database = self.client[database_name]
        collection = database[collection_name]

        existing_document = collection.find_one({"_id": entity_id})

        unique_indexes, non_unique_indexes = self.get_indexes_from_json(database_name, collection_name)

        response = ''

        if self.check_insert(database_name, collection_name, attributes):
            if existing_document is not None:
                raise Exception(f"Document with _id {entity_id} already exists in {collection_name}")

            # handle Non-Unique Indexes
            if non_unique_indexes:
                attributes = str(attributes)
                attributes_split = attributes.split("#")
                for key, value in non_unique_indexes.items():
                    n_index = value.strip("()").split(",")[1:]
                    index_values = [
                        attributes_split[self.get_attribute_position(database_name, collection_name, attr) - 2] for attr
                        in n_index]
                    index_values_str = "_".join(str(v) for v in index_values)
                    collection_index_name = f"{collection_name}_NonUnique_{key}_INDEX"
                    collection_index = database[collection_index_name]

                    try:  # convert the _id into an integer(if it was given as integer) or let it string
                        index_values_str = int(index_values_str)
                    except ValueError:
                        index_values_str = index_values_str

                    index_document = collection_index.find_one({"_id": index_values_str})

                    if index_document is None:
                        data_index = {
                            "_id": index_values_str,
                            "Value": str(entity_id)
                        }
                        collection_index.insert_one(data_index)
                        print(f"Data inserted for NON-UNIQUE INDEX: {entity_id}")
                    else:
                        old_value = str(index_document["Value"])
                        old_values_list = old_value.split('#')
                        if entity_id not in old_values_list:
                            if old_value.endswith('#'):
                                new_value = old_value + f"{entity_id}"
                            else:
                                new_value = old_value + f"#{entity_id}"

                            collection_index.update_one({'_id': index_values_str},
                                                        {'$set': {'Value': new_value}})
                            print(f"Data inserted for NON-UNIQUE INDEX: {entity_id}")

            # handle Unique Indexes
            if unique_indexes:
                attributes_split = attributes.split("#")
                for key, value in unique_indexes.items():
                    n_index = value.strip("()").split(",")[1:]
                    index_values = [
                        attributes_split[self.get_attribute_position(database_name, collection_name, attr) - 2] for attr
                        in n_index]
                    index_values_str = "_".join(str(v) for v in index_values)
                    collection_index_name = f"{collection_name}_Unique_{key}_INDEX"
                    collection_index = database[collection_index_name]

                    try:  # convert the _id into an integer(if it was given as integer) or let it string
                        index_values_str = int(index_values_str)
                    except ValueError:
                        index_values_str = index_values_str

                    index_document = collection_index.find_one({"_id": index_values_str})

                    if index_document is None:
                        data_index = {
                            "_id": index_values_str,
                            "Value": str(entity_id)
                        }
                        collection_index.insert_one(data_index)
                        print(f"Data inserted for UNIQUE INDEX: {entity_id}")
                    else:
                        response += f"Data with _id {index_values_str} for UNIQUE INDEX already exists."

            # handle fk values
            fk_map = self.get_foreign_keys(database_name, collection_name)
            if fk_map:
                attributes_split = attributes.split("#")
                for fk_name, fk_value in fk_map.items():
                    parent_fk_table = fk_value.strip("()").split(",")[0].strip()
                    parent_fk_column = fk_value.strip("()").split(",")[1].strip()

                    fk_file_name = f"FK_constraint_for_{collection_name}_on_{parent_fk_column}_from_{parent_fk_table}_INDEX"
                    pk_key = self.get_primary_key(database_name, parent_fk_table)
                    if pk_key == parent_fk_column:
                        position = self.get_attribute_position(database_name, collection_name, fk_name)
                        value = attributes_split[position - 2]

                    try:  # convert the _id into an integer(if it was given as integer) or let it string
                        value = int(value)
                    except ValueError:
                        value = value

                    collection_fk = database[fk_file_name]
                    fk_document = collection_fk.find_one({"_id": value})

                    if fk_document is None:
                        data_fk = {
                            "_id": value,
                            "Value": str(entity_id)
                        }
                        collection_fk.insert_one(data_fk)
                        print(f"Data inserted with _id {entity_id} for FK: {value}")
                    else:

                        old_value = str(fk_document["Value"])
                        old_values_list = old_value.split('#')
                        if entity_id not in old_values_list:
                            if old_value.endswith('#'):
                                new_value = old_value + f"{entity_id}"
                            else:
                                new_value = old_value + f"#{entity_id}"

                            collection_fk.update_one({'_id': value},
                                                     {'$set': {'Value': new_value}})
                            print(f"Data inserted with _id {value} for FK: {new_value}")

            try:  # convert the _id into an integer(if it was given as integer) or let it string
                entity_id = int(entity_id)
            except ValueError:
                entity_id = entity_id

            # insert into the main collection
            data = {
                "_id": entity_id,
                "Value": str(attributes)
            }
            collection.insert_one(data)
            print(f"Data with id: {entity_id} inserted in {collection}")
            return response

    def delete_data_mongoDB(self, entity_id, database_name, collection_name):
        database = self.client[database_name]
        collection = database[collection_name]

        self.check_delete_entry_fk_constraint(database_name, collection_name, entity_id)

        delete_status_main = collection.delete_one({"_id": entity_id}).deleted_count
        if delete_status_main == 1:
            print(f"Document with _id '{entity_id}' deleted successfully.")
            fk_map = self.get_foreign_keys(database_name, collection_name)
            if fk_map:
                for fk_name, fk_value in fk_map.items():
                    parent_fk_table = fk_value.strip("()").split(",")[0].strip()
                    parent_fk_column = fk_value.strip("()").split(",")[1].strip()

                    fk_file_name = f"FK_constraint_for_{collection_name}_on_{parent_fk_column}_from_{parent_fk_table}_INDEX"

                    self.delete_inner_value_from_collection(database_name, fk_file_name, entity_id)

            prefix = f"{collection_name}"
            suffix = f"INDEX"

            database = self.client[database_name]
            collection_list = database.list_collection_names()

            for collection in collection_list:
                if collection.startswith(prefix) and collection.endswith(suffix):
                    self.delete_inner_value_from_collection(database_name, collection, entity_id)
        else:
            raise Exception(f"No document found with _id '{entity_id}'.")

    def delete_inner_value_from_collection(self, database_name, fk_file_name, entity_id):

        database = self.client[database_name]
        collection_fk = database[fk_file_name]

        delete_status_fk = 0
        cursor = collection_fk.find({})

        found = False
        for document in cursor:
            entry_id = document.get("_id")
            entry_values = document.get("Value")
            if '#' not in entry_values:
                if entry_values == str(entity_id):
                    delete_status_fk = collection_fk.delete_one({"_id": entry_id}).deleted_count
            else:
                values_list = entry_values.split('#')
                new_values_list = ''
                for value in values_list:
                    if value != str(entity_id):
                        new_values_list += value
                        new_values_list += '#'
                    else:
                        found = True
                if found:

                    if new_values_list == '#':
                        collection_fk.delete_one({"_id": entry_id})
                        delete_status_fk = 1
                        break
                    else:
                        collection_fk.update_one({'_id': entry_id},
                                                 {'$set': {'Value': new_values_list}})
                        delete_status_fk = 1
                        break

        if delete_status_fk == 1:
            print(f"Document with _id '{entity_id}' deleted successfully from the FK file.")
        else:
            raise Exception(f"No document found with _id '{entity_id}' in the FK file.")

    def check_delete_entry_fk_constraint(self, database_name, collection_name, entity_id):

        database = self.client[database_name]
        parent_collection = database[collection_name]
        collections = database.list_collection_names()
        fk_collection_suffix = f"from_{collection_name}_INDEX"

        entry_to_be_deleted = parent_collection.find_one({"_id": entity_id})
        can_be_deleted = True
        if entry_to_be_deleted:

            for collection in collections:
                if collection.endswith(fk_collection_suffix):
                    mongo_collection = database[collection]
                    fk_entry = mongo_collection.find_one({"_id": entity_id})
                    if fk_entry is not None:
                        can_be_deleted = False
                        break

            if not can_be_deleted:
                raise Exception(f"The element with id {entity_id} cannot be deleted to the FK constrains!")
            else:
                print(f"The element with id {entity_id} can be deleted successfully")

    def check_drop_table(self, database_name, collection_name):

        database = self.client[database_name]
        collections = database.list_collection_names()
        fk_collection_prefix = f"{collection_name}_FK"

        for collection in collections:
            if collection.startswith(fk_collection_prefix):
                return False
        return True

    def drop_table_mongoDB(self, database_name, collection_name):

        if not self.check_drop_table(database_name, collection_name):
            raise Exception(f"Table {collection_name} cannot be dropped due to FK constraints")
        else:
            database = self.client[database_name]
            collections = database.list_collection_names()

            fk_substring = f"for_{collection_name}_INDEX"
            for collection in collections:
                if collection.startswith(collection_name) or fk_substring in collection:
                    database.drop_collection(collection)
            return True

    def select_data_mongoDB(self, commands, database_name, collection_name):

        database_file_name = f"{database_name.lower()}.json"

        if not self.check_database_existence(database_file_name):
            raise Exception(f"There is no such database: {database_name}")

        database = self.client[database_name]
        collection_list = database.list_collection_names()

        if collection_name not in collection_list:
            raise Exception(f"There is no such table: {collection_name} in {database_name}")

        if "where" not in commands:
            result_entries = self.simple_select_mongoDB(commands, database_name, collection_name)
        else:
            result_entries = self.complex_select_mongoDB(commands, database_name, collection_name)

        return result_entries

    def simple_select_mongoDB(self, commands, database_name, collection_name):

        is_select_all = True if commands[1] == "*" else False
        resulted_entries = ""

        database = self.client[database_name]
        collection = database[collection_name]

        if is_select_all:  # select * from grade on test1
            for document in collection.find():
                value = str(document.get('Value'))
                value = value.replace("#", ", ")
                value = value[0:-2]
                entry_values = "\n" + str(document.get('_id')) + " " + value
                resulted_entries += entry_values

        else:  # select doi, trei from grade on test1

            select_keyword_index = commands.index("select")
            from_keyword = commands.index("from")

            column_names = commands[select_keyword_index + 1: from_keyword][0]
            column_names_list = column_names.split(',')

            resulted_entries = self.parse_attributes(database_name, collection_name, column_names_list)

        return resulted_entries

    def complex_select_mongoDB(self, commands, database_name, collection_name):
        where_keyword_index = commands.index("where")
        on_keyword = commands.index("on")

        resulted_entries = ""

        where_clause = commands[where_keyword_index + 1: on_keyword]

        count_clauses = 1

        existing_attributes = self.get_attributes_list(database_name, collection_name)

        for clause in where_clause:
            if clause == "and":
                count_clauses += 1
            else:
                if '=' in clause:
                    operator = '='
                elif '>' in clause:
                    operator = '>'
                elif '<' in clause:
                    operator = '<'
                else:
                    raise Exception("Invalid operator")

                clause_values = clause.strip().split(operator)
                attribute_name = clause_values[0]
                attribute_value = clause_values[1]

                if attribute_name not in existing_attributes:
                    raise Exception(f"There is no such column {attribute_name} in {collection_name}")

                database = self.client[database_name]

                unique_index_names, non_unique_index_names = self.get_index_names_for_column(database_name,
                                                                                             collection_name,
                                                                                             attribute_name)

                if unique_index_names:
                    for unique_index_name in unique_index_names:
                        index_file_name = f"{collection_name}_Unique_{unique_index_name}_INDEX"
                        collection = database[index_file_name]
                        cursor = collection.find({})
                        for document in cursor:
                            entry_id = document.get("_id")
                            result = self.check_comparison(entry_id, attribute_value, operator)
                            if result:
                                entity_id = document.get('Value')

                                if "#" not in str(entity_id):  # one entry as value
                                    try:
                                        entity_id = int(entity_id)
                                    except ValueError:
                                        entity_id = entity_id
                                    new_collection = database[collection_name]
                                    existing_document = new_collection.find_one({"_id": entity_id})
                                    value = str(existing_document.get('Value'))
                                    value = value.replace("#", ", ")
                                    value = value[0:-2]
                                    entry_values = "\n" + str(existing_document.get("_id")) + " " + value
                                    resulted_entries += entry_values
                                else:  # multiple entries with the same index value
                                    entity_ids = entity_id
                                    entity_ids_list = entity_ids.split("#")
                                    for entry in entity_ids_list:

                                        try:
                                            entry = int(entry)
                                        except ValueError:
                                            entry = entry
                                        new_collection = database[collection_name]
                                        existing_document = new_collection.find_one({"_id": entry})
                                        value = str(existing_document.get('Value'))
                                        value = value.replace("#", ", ")
                                        value = value[0:-2]
                                        entry_values = "\n" + str(existing_document.get("_id")) + " " + value
                                        resulted_entries += entry_values

                if non_unique_index_names:
                    for non_unique_index_name in non_unique_index_names:
                        index_file_name = f"{collection_name}_NonUnique_{non_unique_index_name}_INDEX"
                        collection = database[index_file_name]
                        cursor = collection.find({})  # change to find_one
                        for document in cursor:
                            entry_id = document.get("_id")
                            result = self.check_comparison(entry_id, attribute_value, operator)
                            if result:
                                entity_id = document.get('Value')

                                if "#" not in str(entity_id):  # one entry as value
                                    try:
                                        entity_id = int(entity_id)
                                    except ValueError:
                                        entity_id = entity_id

                                    new_collection = database[collection_name]
                                    existing_document = new_collection.find_one({"_id": entity_id})
                                    value = str(existing_document.get('Value'))
                                    value = value.replace("#", ", ")
                                    value = value[0:-2]
                                    entry_values = "\n" + str(existing_document.get("_id")) + " " + value
                                    resulted_entries += entry_values
                                else:  # multiple entries with the same index value
                                    entity_ids = entity_id
                                    entity_ids_list = entity_ids.split("#")
                                    for entry in entity_ids_list:

                                        try:
                                            entry = int(entry)
                                        except ValueError:
                                            entry = entry

                                        new_collection = database[collection_name]
                                        existing_document = new_collection.find_one({"_id": entry})
                                        value = str(existing_document.get('Value'))
                                        value = value.replace("#", ", ")
                                        value = value[0:-2]
                                        entry_values = "\n" + str(existing_document.get("_id")) + " " + value
                                        resulted_entries += entry_values

                if not unique_index_names and not non_unique_index_names:  # there are no indices for the given column

                    collection = database[collection_name]
                    cursor = collection.find({})
                    pk_key = self.get_primary_key(database_name, collection_name)

                    for document in cursor:
                        entry_id = document.get("_id")

                        if attribute_name != pk_key:
                            position = self.get_attribute_position(database_name, collection_name, attribute_name)

                            entry_value = document.get('Value')

                            if "#" not in str(entry_value):  # one entry as value
                                try:
                                    entry_value = int(entry_value)
                                except ValueError:
                                    entry_value = entry_value

                            value_list = str(entry_value).split("#")
                            value_to_compare = value_list[position - 2]
                            result = self.check_comparison(value_to_compare, attribute_value, operator)
                        else:
                            try:
                                attribute_value = int(attribute_value)
                            except ValueError:
                                attribute_value = attribute_value
                            result = self.check_comparison(entry_id, attribute_value, operator)
                        if result:
                            value = document.get('Value')
                            value = value.replace("#", ", ")
                            value = value[0:-2]
                            entry_values = "\n" + str(entry_id) + " " + value
                            resulted_entries += entry_values

            return resulted_entries

    def get_index_names_for_column(self, database_name, collection_name, attribute_name):

        unique_indexes, non_unique_indexes = self.get_indexes_from_json(database_name, collection_name)
        unique_index_names = []
        non_unique_index_names = []

        for unique_index in unique_indexes.items():
            index_name = unique_index[0]
            index_collections = unique_index[1]

            if attribute_name in index_collections:
                unique_index_names.append(index_name)

        for non_unique_index in non_unique_indexes.items():
            index_name = non_unique_index[0]
            index_collections = non_unique_index[1]

            if attribute_name in index_collections:
                non_unique_index_names.append(index_name)

        return unique_index_names, non_unique_index_names

    def parse_attributes(self, database_name, collection_name, column_list):

        pk_key = self.get_primary_key(database_name, collection_name)

        database = self.client[database_name]
        collection = database[collection_name]

        result_data = {}

        for column in column_list:

            result_data[column] = []
            if pk_key == column:
                position = -10
            else:
                position = self.get_attribute_position(database_name, collection_name, column) - 1

            if position == -2:
                raise Exception(f"There is no such column {column} in {collection_name}")

            for document in collection.find():
                entry_id = document.get("_id")
                entry_value = str(document.get('Value'))

                if position == -10:
                    result_data[column].append(str(entry_id))
                else:
                    entry_values_list = entry_value.split("#")[position - 1].strip("#")
                    result_data[column].append(entry_values_list)

        final = ""

        for i in range(len(result_data[column_list[0]])):  # Assuming all columns have the same number of entries
            entry_values = [result_data[column][i] for column in column_list]
            final += f"\n{', '.join(map(str, entry_values))}"

        return final

    def get_attributes_list(self, database_name, collection_name):
        database_file_name = f"{database_name.lower()}.json"
        existing_attributes = []
        if self.check_database_existence(database_file_name):
            file_path = os.path.join(self.json_directory, database_file_name)

            with open(file_path, 'r') as json_file:
                json_data = json.load(json_file)

                if database_name in json_data:
                    collections = json_data[database_name]['Tables']
                if collection_name in collections:
                    attributes = collections[collection_name]['Attributes']
                    for attribute in attributes.keys():
                        existing_attributes.append(attribute)
        return existing_attributes

    def join(self, commands, database_name):  # select * from ttt join student on ttt.numefk=student.nume in test1
        commands = commands.split()

        join_keyword_index = commands.index("join")
        first_collection_name = commands[join_keyword_index - 1]
        second_collection_name = commands[join_keyword_index + 1]

        database = self.client[database_name]
        collections_list = database.list_collection_names()

        if first_collection_name not in collections_list or second_collection_name not in collections_list:
            raise Exception(
                f"Both tables {first_collection_name, second_collection_name} must exist in the database {database_name}!")

        join_columns = ''
        for command in commands:
            if "=" in command:
                join_columns = command

        if not join_columns:
            raise Exception("No condition for join!")

        join_columns = join_columns.split("=")
        left_hand_side = join_columns[0]
        right_hand_side = join_columns[1]

        left_collection_name, left_collection_attribute = left_hand_side.split('.')
        right_collection_name, right_collection_attribute = right_hand_side.split('.')

        if left_collection_name == first_collection_name and right_collection_name == second_collection_name:
            left_existing_attributes = self.get_attributes_list(database_name, left_collection_name)
            right_existing_attributes = self.get_attributes_list(database_name, right_collection_name)

            if left_collection_attribute not in left_existing_attributes:
                raise Exception(
                    f"The join column {left_collection_attribute} does not exist in the {left_collection_name} ")

            if right_collection_attribute not in right_existing_attributes:
                raise Exception(
                    f"The join column {right_collection_attribute} does not exist in the {right_collection_name} ")

            print("The join conditions are correct!")
        raise Exception(f"The join tables are not the same")

    def insert_old_entries_index(self, database_name, collection_name, index_value, index_table_name, index_type):

        database = self.client[database_name]
        collection = database[collection_name]
        index_collection = database[index_table_name]

        database_file_name = f"{database_name.lower()}.json"
        file_path = os.path.join(self.json_directory, database_file_name)

        with open(file_path, 'r') as json_file:
            json_data = json.load(json_file)

            index_column = ''

            for database in json_data.values():
                for index_key, index_pair in database['Indexes'][index_type].items():
                    if index_key == index_value:
                        index_column = index_pair.strip("()").split(',')[1].strip()

            if index_column != '':

                ok = 0
                cursor = collection.find({})
                pk = self.get_primary_key(database_name, collection_name)

                if pk == index_column:
                    position = 0
                else:
                    position = self.get_attribute_position(database_name, collection_name, index_column)

                for document in cursor:
                    ok = 1
                    entry_id = document.get("_id")
                    entry_value = document.get("Value")

                    if position == 0:
                        entry_id_to_be_inserted = entry_id
                        entry_value_to_be_inserted = entry_value
                    else:
                        entry_value = str(entry_value)
                        entry_id_to_be_inserted = entry_value.split('#')[position - 2]
                        entry_value_to_be_inserted = str(entry_id)

                        try:
                            entry_id_to_be_inserted = int(entry_id_to_be_inserted)
                        except ValueError:
                            entry_id_to_be_inserted = entry_id_to_be_inserted

                    if index_type == "NonUnique":
                        index_document = index_collection.find_one({"_id": entry_id_to_be_inserted})

                        if index_document is None:
                            data_index = {
                                "_id": entry_id_to_be_inserted,
                                "Value": entry_value_to_be_inserted
                            }
                            index_collection.insert_one(data_index)
                            print(
                                f"Data inserted _id {entry_id_to_be_inserted} for NON-UNIQUE INDEX: {entry_id_to_be_inserted}")
                        else:

                            old_value = str(index_document["Value"])
                            old_values_list = old_value.split('#')
                            if entry_value_to_be_inserted not in old_values_list:

                                if old_value.endswith('#'):
                                    new_value = old_value + f"{entry_value_to_be_inserted}"
                                else:
                                    new_value = old_value + f"#{entry_value_to_be_inserted}"
                                index_collection.update_one({'_id': entry_id_to_be_inserted},
                                                            {'$set': {'Value': new_value}})
                                print(
                                    f"Data inserted with {entry_id_to_be_inserted} for NON-UNIQUE INDEX: {entry_id_to_be_inserted}")

                        # handle Unique Indexes
                    if index_type == "Unique":

                        index_document = index_collection.find_one({"_id": entry_id_to_be_inserted})
                        if index_document is None:
                            data_index = {
                                "_id": entry_id_to_be_inserted,
                                "Value": entry_value_to_be_inserted
                            }
                            index_collection.insert_one(data_index)
                            print(f"Data inserted with _id {entry_id_to_be_inserted} for UNIQUE INDEX")
                        else:
                            print(f"Data with _id {entry_id_to_be_inserted} for UNIQUE INDEX already exists.")
                if ok == 0:
                    print(f"No old entries to copy into {index_table_name}")
