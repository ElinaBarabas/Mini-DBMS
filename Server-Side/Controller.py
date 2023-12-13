from ClientMongo import *


class Controller:

    def __init__(self, command_type, instance_type, instance_name):
        self.command_type = command_type
        self.instance_type = instance_type
        self.instance_name = instance_name

        self.json_directory = ClientMongo.set_json_files_path()
        self.json_files = [file for file in os.listdir(self.json_directory) if file.endswith('.json')]

    @staticmethod
    def process_brackets_fields(client_data):

        left_bracket = "("
        right_bracket = ")"

        left_bracket_index = client_data.index(left_bracket)
        right_bracket_index = client_data.index(right_bracket)

        table_data = client_data[left_bracket_index + 1: right_bracket_index]

        return list(map(lambda x: x.strip(), table_data.split(',')))

    def check_database_existence(self, database_file):
        for current_database_file in self.json_files:
            if current_database_file == database_file:
                return True
        return False

    def create_database(self):

        database_file_name = f"{self.instance_name.lower()}.json"

        if self.check_database_existence(database_file_name):
            raise Exception(f"There is already one database with the same name ({database_file_name})")
        else:
            file_path = os.path.join(self.json_directory, database_file_name)

            data = {
                self.instance_name: {
                    "Tables": {},
                    "Indexes": {
                        "Unique": {},
                        "NonUnique": {}
                    }
                }
            }

            try:
                with open(file_path, 'w') as json_file:
                    json.dump(data, json_file, indent=4)
                print(f"JSON file created: {self.instance_name}.json")
            except Exception as e:
                print("The Database was not created")
                print(e)

    def drop_database(self):

        database_file_name = f"{self.instance_name.lower()}.json"

        if not self.check_database_existence(database_file_name):
            raise Exception(f"There is no database with this name {database_file_name}")
        else:
            file_path = os.path.join(self.json_directory, database_file_name)

            try:
                os.remove(file_path)
            except Exception as e:
                print("The Database was not deleted")
                print(e)

    def create_table(self, client_data):
        new_table = client_data.split(" ")[2]
        table_fields = self.process_brackets_fields(client_data)
        database_name = client_data.split("on")[1].strip()
        fields_map = {}
        pk_value = ""
        fk_values = []
        unique_map = {}

        for field in table_fields:
            attribute_pair = field.split(" ")

            if len(attribute_pair) < 2:
                raise Exception("You must provide the field type and value")

            if field.__contains__("PK"):
                if not attribute_pair[1] == "int" and not attribute_pair[1] == "varchar":
                    raise Exception("Field types must be int or varchar")
                fields_map[attribute_pair[2]] = attribute_pair[1]
                pk_value = attribute_pair[2]

            elif field.__contains__("FK"):
                if not attribute_pair[1] == "int" and not attribute_pair[1] == "varchar":
                    raise Exception("Field types must be int or varchar")
                fields_map[attribute_pair[2]] = attribute_pair[1]
                fk_value = attribute_pair[2]
                table_name, table_column = field.split("ref ")[1].split("-")
                fk_builder = {"fk_name": fk_value, "table": table_name, "column": table_column}
                fk_values.append(fk_builder)

            else:
                if not attribute_pair[0] == "int" and not attribute_pair[0] == "varchar":
                    raise Exception("Field types must be int or varchar")
                fields_map[attribute_pair[1]] = attribute_pair[0]
                if field.__contains__("unique"):
                    unique_index_name = f"{new_table}_{attribute_pair[1]}"
                    unique_map[unique_index_name] = f"({new_table}, {attribute_pair[1]})"

        database_file_name = f"{database_name.lower()}.json"

        if not self.check_database_existence(database_file_name):
            raise Exception(f"There is no database with this name {database_file_name}")
        else:
            file_path = os.path.join(self.json_directory, database_file_name)

        try:
            with open(file_path, 'r') as json_file:
                data = json.load(json_file)

                if database_name not in data:
                    data[database_name] = {
                        "Tables": {},
                        "Indexes": {
                            "Unique": {},
                            "NonUnique": {}
                        }
                    }

                data[database_name]["Tables"][self.instance_name] = {
                    "Attributes": fields_map,
                    "Keys": {
                        "PK": pk_value,
                        "FK": {}
                    }
                }

                if fk_values:
                    for fk in fk_values:
                        fk_name = fk["fk_name"]
                        fk_table = fk["table"]
                        fk_column = fk["column"]
                        fk_builder = f"({fk_table}, {fk_column})"
                        data[database_name]["Tables"][self.instance_name]["Keys"]["FK"][fk_name] = fk_builder

                if unique_map:
                    data[database_name]["Indexes"]["Unique"].update(unique_map)

                with open(file_path, 'w') as json_database_file:
                    json.dump(data, json_database_file, indent=4)

                print(f"Table '{self.instance_name}' added to {file_path}")

        except Exception as e:
            print(f"An error occurred: {str(e)}")

    def delete_table(self, database_name):
        database_file_name = f"{database_name.lower()}.json"

        if not self.check_database_existence(database_file_name):
            raise Exception(f"There is no database with this name {database_file_name}")

        file_path = os.path.join(self.json_directory, database_file_name)

        with open(file_path, 'r') as json_file:
            data = json.load(json_file)

        if "Tables" in data[database_name]:
            if self.instance_name in data[database_name]["Tables"]:
                del data[database_name]["Tables"][self.instance_name]

                for database in data.values():
                    for index_type, indexes in database['Indexes'].items():
                        for index_name, index_definition in list(indexes.items()):
                            value = index_definition.strip('()').split(',')[0]
                            if value == self.instance_name:
                                del indexes[index_name]

                # Write the modified data back to the file
                with open(file_path, 'w') as json_database_file:
                    json.dump(data, json_database_file, indent=4)

                print(f"Table '{self.instance_name}' deleted from {file_path}")
            else:
                raise Exception(f"Table '{self.instance_name}' not found in {database_file_name}")
        else:
            raise Exception(f"No 'Tables' key found in {database_file_name}")

    def create_index(self, database_name, table_name, index_type, index_name, client_request):

        index_fields = self.process_brackets_fields(client_request)

        if len(index_fields) < 1:
            raise Exception("You must provide at least one parameter for the index creation")

        database_file_name = f"{database_name.lower()}.json"

        if not self.check_database_existence(database_file_name):
            raise Exception(f"There is no database with this name {database_file_name}")

        file_path = os.path.join(self.json_directory, database_file_name)

        with open(file_path, 'r') as json_file:
            data = json.load(json_file)

            tuple_str = "(" + table_name
            if table_name in data[database_name]["Tables"]:
                for column_name in index_fields:
                    if column_name in data[database_name]["Tables"][table_name]["Attributes"]:
                        tuple_str = tuple_str + ", " + column_name
                    else:
                        raise Exception(f"There is no such column: {column_name} in table: {table_name}")

                tuple_str = tuple_str + ")"
                if index_type == "unique":
                    data[database_name]["Indexes"]["Unique"][index_name] = tuple_str
                if index_type == "nonunique":
                    data[database_name]["Indexes"]["NonUnique"][index_name] = tuple_str

                with open(file_path, 'w') as json_database_file:
                    json.dump(data, json_database_file, indent=4)

                print(f"Index '{index_name}' added to {file_path}")
            else:
                raise Exception(f"There is no such table {table_name}")

    def get_table_attributes(self, database_name, table_name):

        database_file_name = f"{database_name.lower()}.json"

        if not self.check_database_existence(database_file_name):
            raise Exception(f"There is no database with this name {database_file_name}")

        file_path = os.path.join(self.json_directory, database_file_name)

        with open(file_path, 'r') as json_file:
            data = json.load(json_file)

            if database_name in data:
                tables = data[database_name].get('Tables', {})

                if table_name in tables:
                    table_data = tables[table_name]
                    keys_data = table_data.get('Keys', {})

                    pk_key = keys_data.get('PK', None)
                    attributes_data = table_data.get('Attributes', {})

                    if pk_key in attributes_data:
                        pk_value = attributes_data.pop(pk_key)

                    result = {"PK": {pk_key: pk_value}, "Attributes": attributes_data}
                    return result
            else:
                return None

    def mongoDB_format(self, database_name, table_name, client_data):

        entity_id = ""
        values = self.process_brackets_fields(client_data)
        if len(values) <= 1:
            return "You need to specify the values of attributes separated by a coma"
        else:
            attributes_json = self.get_table_attributes(database_name, table_name)
            if attributes_json is None:
                raise Exception("There is no database/table with this name")
            else:
                pk_data = attributes_json["PK"]
                attributes_data = attributes_json["Attributes"]

                for key in pk_data:
                    pk_name, pk_type = key, pk_data[key]

                    if pk_type.lower() == "int" and isinstance(int(values[0]), int):  # verify if it must be an integer
                        entity_id = int(values[0])
                    elif pk_type.lower() == "varchar" and isinstance(values[0], str):  # verify if it must be string
                        entity_id = values[0]
                    else:
                        raise Exception("Invalid PK Type")
                values.remove(str(entity_id))
                attributes = ""
                for value, (attribute_name, attribute_type) in zip(values, attributes_data.items()):
                    if attribute_type.lower() == "int" and isinstance(int(value),
                                                                      int):  # verify if it must be an integer
                        attributes += str(value) + '#'
                    elif attribute_type.lower() == "varchar" and isinstance(value, str):
                        attributes += str(value) + '#'
                    else:
                        raise Exception("Invalid type, please make sure you entered correctly the attributes types")
                return entity_id, attributes
