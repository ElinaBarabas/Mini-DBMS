def validate_command_type(command_type, command_parts):

    if command_type == "create" or command_type == "drop":
        instance_type = command_parts[1].lower()
        valid_instance_types = ["database", "table", "index"]

        if instance_type not in valid_instance_types:
            raise Exception("Instance type for create/drop is required (database, table, index)")
        # else:
        #     if command_parts[0].lower() == "create" and command_parts[1].lower() == "table":
        #         columns = input("Enter the column names (comma-separated): ").split(",")
        #         print(columns)
        #         table_data = {"columns": columns}
        #         table_data_json = json.dumps(table_data)
        #


class InputManipulator:
    def __init__(self, client_message):
        client_message = client_message.strip()
        self.client_message = client_message
        self.validate_input()

    def validate_input(self):
        inputCommands = self.client_message.split(" ")
        if len(inputCommands) < 3:
            raise Exception("The command should contain at least three arguments")
        else:
            command_type = inputCommands[0].lower()
            valid_command_types = ["create", "drop", "insert", "delete", "select"]

            if command_type not in valid_command_types:
                raise Exception("Client must enter the command type (create, drop, insert, delete, select)")
            else:
                validate_command_type(command_type, inputCommands)

# try:
#     client_message = "create database mydb"
#     input_manipulator = InputManipulator(client_message)
#     print("Input is valid.")
# except Exception as e:
#     print("Input validation error:", str(e))
