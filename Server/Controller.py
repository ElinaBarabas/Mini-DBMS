import json
import os
import string


class Controller:
    def __init__(self, command_type, instance_type, instance_name):
        self.command_type = command_type
        self.instance_type = instance_type
        self.instance_name = instance_name
        self.data_base = {}

    def create_database(self):
        # file_directory = f"C:\\Users\\user\\Desktop\\Mini dbsm\\json"

        file_directory = os.getcwd()
        file_directory = os.path.abspath(os.path.join(file_directory, os.pardir))
        file_directory += f"\\json\\"

        current_directory = file_directory

        file_name = f"{self.instance_name}.json"
        file_path = os.path.join(file_directory, file_name)

        self.instance_name = self.instance_name.upper()

        existing_files = os.listdir(current_directory)
        print(existing_files)

        for file in existing_files:
            if file == file_name:
                raise Exception(f"There is already one database with the same name ({file})")

        data = {
            self.instance_name: {
                "Tables": {},
                "Indexes": {}
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

        file_directory = os.getcwd()
        file_directory = os.path.abspath(os.path.join(file_directory, os.pardir))
        file_directory += f"\\json\\"

        current_directory = file_directory

        file_name = f"{self.instance_name}.json"
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

        # print(left_bracket_index)
        # print(right_bracket_index)

        table_data = client_data[left_bracket_index + 1: right_bracket_index]
        # print(table_data)

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

            data[database_name.upper()]["Tables"][self.instance_name] = \
                {
                    "Attributes": {},
                    "Keys": {
                        "PK": {},
                        "FK": {}

                    }
                }

            data[database_name.upper()]["Tables"][self.instance_name]["Attributes"] = fields_map

            if pk_value != "":
                data[database_name.upper()]["Tables"][self.instance_name]["Keys"]["PK"] = pk_value

            if fk_value != "":
                if not (data[database_name.upper()]["Tables"][table_name]):
                    print(f"There is no such table {table_name}")
                if not (table_column in data[database_name.upper()]["Tables"][table_name]):
                    print(f"There is no such column {table_column} in table: {table_name}")

                # tuple_value = (table_name,table_column)
                fk_tostring = "(" + table_name + ", " + table_column + ")"
                print(fk_tostring)
                if isinstance(fk_tostring, str):
                    print("my_variable is a string")
                else:
                    print("my_variable is not a string")
                data[database_name.upper()]["Tables"][self.instance_name]["Keys"]["FK"][fk_value] = fk_tostring

            with open(file_path, 'w') as json_file:
                json.dump(data, json_file, indent=4)

            print(f"Table '{self.instance_name}' added to {file_path}")

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
        print(data)
        # Check if the key "Tables" exists.
        if "Tables" in data[database_name]:
            if self.instance_name in data[database_name]["Tables"]:
                del data[database_name]["Tables"][self.instance_name]

                with open(file_path, 'w') as json_file:
                    json.dump(data, json_file, indent=4)

                print(f"Table '{self.instance_name}' deleted from {file_path}")
            else:
                raise Exception(f"Table '{self.instance_name}' not found in {file_name}")
        else:
            raise Exception(f"No 'Tables' key found in {file_name}")

        # except FileNotFoundError:
        #     print(f"File '{file_path}' not found.")
        # except json.JSONDecodeError:
        #     print(f"Invalid JSON in file '{file_path}'")
        # except Exception as e:
        #     print(f"An error occurred: {str(e)}")

    def create_index(self, client_request):

        index_fields = self.process_brackets_fields(client_request)

        if len(index_fields) != 2:
            raise Exception("You must provide only two parameters for the index creation (table_name, column_name)")

        [table_name, column_name] = index_fields

        database_name = client_request.split("on")[1]
        database_name = database_name.strip()
        print(database_name)

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

        if table_name in data[database_name]["Tables"]:
            if column_name in data[database_name]["Tables"][table_name]["Attributes"]:
                tuple_str = "(" + table_name + ", " + column_name + ")"
                data[database_name.upper()]["Indexes"][self.instance_name] = tuple_str

                with open(file_path, 'w') as json_file:
                    json.dump(data, json_file, indent=4)

                print(f"Index '{self.instance_name}' added to {file_path}")
            else:
                raise Exception(f"There is no such column: {column_name} in table: {table_name}")

        else:
            raise Exception(f"There is no such table {table_name}")

        # except json.JSONDecodeError:
        #     print(f"Invalid JSON in file '{file_path}'")
        # except Exception as e:
        #     print(f"An error occurred: {str(e)}")
