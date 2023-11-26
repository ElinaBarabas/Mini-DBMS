import json
import os
import string


class Controller:
    def __init__(self, command_type, instance_type, instance_name):
        self.command_type = command_type
        self.instance_type = instance_type
        self.index_name = instance_name
        self.data_base = {}

    def create_database(self):
        # file_directory = f"C:\\Users\\user\\Desktop\\Mini dbsm\\json"

        file_directory = os.getcwd()
        file_directory = os.path.abspath(os.path.join(file_directory, os.pardir))
        file_directory += f"\\json\\"

        current_directory = file_directory

        file_name = f"{self.index_name}.json"
        file_path = os.path.join(file_directory, file_name)

        self.index_name = self.index_name.upper()

        existing_files = os.listdir(current_directory)
        print(existing_files)

        for file in existing_files:
            if file == file_name:
                raise Exception(f"There is already one database with the same name ({file})")

        data = {
            self.index_name: {
                "Tables": {},
                "Indexes": {
                    "Unique":{},
                    "NonUnique":{}
                }
            }
        }
        try:
            with open(file_path, 'w') as json_file:
                json.dump(data, json_file, indent=4)
            print(f"JSON file created: {self.index_name}.json")
        except Exception as e:
            print("The Database was not created")
            print(e)

    def drop_database(self):

        file_directory = os.getcwd()
        file_directory = os.path.abspath(os.path.join(file_directory, os.pardir))
        file_directory += f"\\json\\"

        current_directory = file_directory

        file_name = f"{self.index_name}.json"
        file_path = os.path.join(file_directory, file_name)

        existing_files = os.listdir(current_directory)
        print(existing_files)

        if file_name not in existing_files:
            raise Exception("No such file/directory exists")

        os.remove(file_path)

    def process_brackets_fields(self, client_data):

        left_bracket = "("
        right_bracket = ")"

        left_bracket_index = client_data.index(left_bracket)
        right_bracket_index = client_data.index(right_bracket)

        table_data = client_data[left_bracket_index + 1: right_bracket_index]

        return list(map(lambda x: x.strip(), table_data.split(',')))

    def create_table(self, client_data):

        table_fields = self.process_brackets_fields(client_data)
        database_name = client_data.split("on")[1]
        database_name = database_name.strip()
        fields_map = {}
        pk_value = ""
        fk_value = ""
        for field in table_fields:
            attribute_pair = field.split(" ")  # PK INT V1
            if len(attribute_pair) < 2:
                raise Exception("You must provide the field type and value")

            if field.__contains__("PK"):  # create table table1 (PK int 1, int 2, varchar cc) on test1
                if not attribute_pair[1] == "int" and not attribute_pair[1] == "varchar":
                    raise Exception("Field types must be int or varchar")
                fields_map[attribute_pair[2]] = attribute_pair[1]
                pk_value = attribute_pair[2]
            elif field.__contains__("FK"):
                if not attribute_pair[1] == "int" and not attribute_pair[1] == "varchar":
                    raise Exception("Field types must be int or varchar")
                fields_map[attribute_pair[2]] = attribute_pair[1]
                fk_value = attribute_pair[2]
                table_name = field.split("ref ")[1].split("-")[
                    0]  # create table table3 (int 1, FK int val2 ref table_name-table_column, varchar cc) on test1
                table_column = field.split("ref")[1].split("-")[1]

                print(table_name, "----------------", table_column)
            else:

                if not attribute_pair[0] == "int" and not attribute_pair[0] == "varchar":
                    raise Exception("Field types must be int or varchar")
                fields_map[attribute_pair[1]] = attribute_pair[0]

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

            data[database_name.upper()]["Tables"][self.index_name] = \
                {
                    "Attributes": {},
                    "Keys": {
                        "PK": {},
                        "FK": {}

                    }
                }

            data[database_name.upper()]["Tables"][self.index_name]["Attributes"] = fields_map

            if pk_value != "":
                data[database_name.upper()]["Tables"][self.index_name]["Keys"]["PK"] = pk_value

            if fk_value != "":
                if not (data[database_name.upper()]["Tables"][table_name]):
                    print(f"There is no such table {table_name}")
                if not (table_column in data[database_name.upper()]["Tables"][table_name]):
                    print(f"There is no such column {table_column} in table: {table_name}")

                fk_tostring = "(" + table_name + ", " + table_column + ")"
                print(fk_tostring)
                if isinstance(fk_tostring, str):
                    print("my_variable is a string")
                else:
                    print("my_variable is not a string")
                data[database_name.upper()]["Tables"][self.index_name]["Keys"]["FK"][fk_value] = fk_tostring

            with open(file_path, 'w') as json_file:
                json.dump(data, json_file, indent=4)

            print(f"Table '{self.index_name}' added to {file_path}")

        # except json.JSONDecodeError:
        #     print(f"Invalid JSON in file '{file_path}'")
        except Exception as e:
            print(f"An error occurred: {str(e)}")

    def delete_table(self, database_name):
        # file_directory = f"C:\\Users\\user\\Desktop\\Mini dbsm\\json"

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
            data = json.load(json_file)


        if "Tables" in data[database_name]:
            if self.index_name in data[database_name]["Tables"]:
                del data[database_name]["Tables"][self.index_name]

                with open(file_path, 'w') as json_file:
                    json.dump(data, json_file, indent=4)

                print(f"Table '{self.index_name}' deleted from {file_path}")
            else:
                raise Exception(f"Table '{self.index_name}' not found in {file_name}")
        else:
            raise Exception(f"No 'Tables' key found in {file_name}")

    def create_index(self, database_name,table_name,index_type,index_name,client_request):

        index_fields = self.process_brackets_fields(client_request)


        if len(index_fields) < 1:
            raise Exception("You must provide at leat one parameter for the index creation")

        file_directory = os.getcwd()
        file_directory = os.path.abspath(os.path.join(file_directory, os.pardir))
        file_directory += f"\\json\\"

        file_name = f"{database_name}.json"
        file_path = os.path.join(file_directory, file_name)

        if not os.path.isfile(file_path):
            raise Exception(f"Database '{database_name}' does not exist.")

        database_name = database_name.upper()
        with open(file_path, 'r') as json_file:
            data = json.load(json_file)
        tuple_str = "(" + table_name
        if table_name in data[database_name]["Tables"]:
            for column_name in index_fields:
                if column_name in data[database_name]["Tables"][table_name]["Attributes"]:
                    tuple_str = tuple_str + ", " + column_name
                else:
                    raise Exception(f"There is no such column: {column_name} in table: {table_name}")

            tuple_str = tuple_str +  ")"
            if index_type == "unique":
                data[database_name.upper()]["Indexes"]["Unique"][index_name] = tuple_str
            if index_type == "nonunique":
                data[database_name.upper()]["Indexes"]["NonUnique"][index_name] = tuple_str

            with open(file_path, 'w') as json_file:
                json.dump(data, json_file, indent=4)

            print(f"Index '{index_name}' added to {file_path}")

        else:
            raise Exception(f"There is no such table {table_name}")

    def get_table_attributes(self,db_name, table_name):
        file_directory = os.getcwd()
        file_directory = os.path.abspath(os.path.join(file_directory, os.pardir))
        file_directory += f"\\json\\"

        file_name = f"{db_name.lower()}.json"

        current_directory = file_directory
        existing_files = os.listdir(current_directory)

        database_exists = 0

        for file in existing_files:
            if file == file_name:
                database_exists = 1

        if database_exists == 0:
            raise Exception(f"There is no database with this name {db_name}")

        file_path = os.path.join(file_directory, file_name)

        db_name = db_name.upper()

        with open(file_path, 'r') as json_file:
            json_data = json.load(json_file)

        if db_name in json_data:
            tables = json_data[db_name].get('Tables', {})

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

    def mongoDB_format (self,db_name,table_name,client_data):
        values = self.process_brackets_fields(client_data)
        if len(values) <= 1:
            return "You need to specify the values of attributes separated by a coma"
        else:
            attributes_json = self.get_table_attributes(db_name, table_name)
            if attributes_json is None:
                raise Exception("There is no database/table with this name")
            else:
                print(attributes_json)

            if attributes_json:
                pk_data = attributes_json["PK"]
                attributes_data = attributes_json["Attributes"]
                pk_type, pk_name = None, None

                for key in pk_data:
                    pk_name, pk_type = key, pk_data[key]

                print("PK Type:", pk_type)
                print("PK Name:", pk_name)
                print(type(values[0]))
                if pk_type.lower() == "int" and isinstance(int(values[0]),int):  # verify if it must be an integer
                    id = int(values[0])
                elif pk_type.lower() == "varchar" and isinstance(values[0], str):  # verify if it must be strinbg
                        id = values[0]
                else:
                        raise Exception("Invalid PK Type")
                values.remove(str(id))
                attributes =""
                print(attributes_data.items())
                for value,(attribute_name,attribute_type) in zip (values,attributes_data.items()):
                    print(attribute_type.lower())
                    #print(value)
                    if attribute_type.lower() == "int" and isinstance(int(value), int):  # verify if it must be an integer
                        attributes += str(value) + '#'
                    elif attribute_type.lower() == "varchar" and isinstance(value,str):
                        attributes += str(value) + '#'
                    else:
                        raise Exception ("Invalid type, please make sure you entered correctly the attributes types")
            else:
                print("Table not found or no PK defined.")

            return id,attributes

