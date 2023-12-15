import socket
from Controller import *


def server_program():
    mongo = ClientMongo()
    try:

        host = socket.gethostname()
        port = 5235
        server_socket = socket.socket()
        server_socket.bind((host, port))
        server_socket.listen(2)
        conn, address = server_socket.accept()
        print("Connection from: " + str(address))
        mongo.update_mongoDB()
        data = ""

        while data != "exit":

            data = ""
            try:

                data = conn.recv(1024).decode()
                client_request = str(data)
                commands = client_request.split(" ")
                command_type = commands[0].lower()
                instance_type = commands[1].lower()
                instance_name = commands[2].lower()

                controller = Controller(command_type, instance_type, instance_name)

                if command_type == "create" and instance_type == "database":
                    controller.create_database()
                    mongo.update_mongoDB()

                elif command_type == "drop" and instance_type == "database":
                    controller.drop_database()
                    mongo.update_mongoDB()

                elif command_type == "create" and instance_type == "table":  # # create table table_name (int 1, FK int val2 ref table_name-table_column, varchar cc) on db_name
                    controller.create_table(client_request)
                    mongo.update_mongoDB()

                elif command_type == "drop" and instance_type == "table":  # drop table table_name on database
                    db_name = commands[4]
                    if mongo.drop_table_mongoDB(db_name, instance_name):
                        controller.delete_table(db_name)
                    mongo.update_mongoDB()

                elif command_type == "create" and instance_type == "index":  # create  index unique/non unique index_name on table_name (column_name,column_name) on database_name
                    table_name = commands[5].lower()
                    db_name = commands[len(commands) - 1].lower()
                    index_type = commands[2].lower()
                    index_name = commands[3].lower()
                    controller.create_index(db_name, table_name, index_type, index_name, client_request)
                    mongo.update_mongoDB()

                elif command_type == "insert" and instance_type == "into":  # insert into db_name table_name values (1,2,3)
                    table_name = commands[3].lower()
                    _id, attributes = controller.mongoDB_format(instance_name, table_name, client_request)
                    mongo.insert_data_mongoDB(_id, attributes, instance_name, table_name)
                    mongo.update_mongoDB()

                elif command_type == "delete" and instance_type == "from":  # delete from db_name table_name value id_value
                    table_name = commands[3].lower()
                    _id = commands[5].lower()
                    try:  # convert the _id into an integer(if it was given as integer) or let it string
                        id_value = int(_id)
                    except ValueError:
                        id_value = _id
                    mongo.delete_data_mongoDB(id_value, instance_name, table_name)

                elif command_type == "select":  # select * from grade on table_name
                    instance_name_index = commands.index("from") + 1
                    table_name = commands[instance_name_index]

                    database_name_index = commands.index("on") + 1
                    database_name = commands[database_name_index]

                    result_entries = mongo.select_data_mongoDB(commands, database_name, table_name)
                    data = result_entries
                    # conn.send(result_entries.encode())

                print("\n>> Command executed")

                if client_request != "exit" and not data:
                    data = "completed"
                elif not data:
                    data = "exit"

                if data.startswith(command_type):
                    data = "completed"
                conn.send(data.encode())

            except Exception as e:
                print("An error occurred:", str(e))
                error = str(e)
                conn.send(error.encode())

    except Exception as e:
        print("An error occurred outside the loop:", str(e))
    finally:
        mongo.close_mongoDB()


if __name__ == "__main__":
    server_program()
