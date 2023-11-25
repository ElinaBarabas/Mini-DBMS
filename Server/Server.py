import socket
import json
from Controller import Controller
from ClientMongo import ClientMongo


def server_program():
    global conn
    try:

        host = socket.gethostname()
        port = 5235
        server_socket = socket.socket()
        server_socket.bind((host, port))
        server_socket.listen(2)
        conn, address = server_socket.accept()
        print("Connection from: " + str(address))
        mongo = ClientMongo()
        mongo.update_mongoDB()
        data = ""


        while data != "exit":

            try:

                data = conn.recv(1024).decode()
                client_request = str(data)
                commands = client_request.split(" ")
                command_type = commands[0].lower()
                instance_type = commands[1].lower()
                instance_name = commands[2].lower()
                print(instance_name)

                controller = Controller(command_type, instance_type, instance_name)

                if command_type == "create" and instance_type == "database":
                    controller.create_database()
                    mongo.update_mongoDB()

                elif command_type == "drop" and instance_type == "database":
                    controller.drop_database()
                    mongo.update_mongoDB()

                elif command_type == "create" and instance_type == "table":   # # create table table_name (int 1, FK int val2 ref table_name-table_column, varchar cc) on db_name
                    controller.create_table(client_request)
                    mongo.update_mongoDB()

                elif command_type == "drop" and instance_type == "table":    # drop table table_name on database
                    db_name = commands[4].upper()
                    controller.delete_table(db_name)
                    mongo.update_mongoDB()

                elif command_type == "create" and instance_type == "index":   # create index index_name (table_name,column_name) on database_name
                    controller.create_index(client_request)
                    mongo.update_mongoDB()

                elif command_type == "insert" and instance_type == "into":   # insert into db_name table_name values (1,2,3)
                    table_name = commands[3].lower()
                    id,attributes = controller.mongoDB_format(instance_name,table_name,client_request)
                    mongo.add_mongoDB(id,attributes,instance_name,table_name)
                    mongo.update_mongoDB()

                elif command_type == "delete" and instance_type == "from": #delete from db_name table_name value id_value
                    table_name = commands[3].lower()
                    id = commands[5].lower()
                    try:  # convert the _id into an integer(if it was given as integer) or let it string
                        id_value = int(id)
                    except ValueError:
                             id_value = id
                    mongo.delete_mongoDB(id_value,instance_name,table_name)


                print("\n>> Command executed")

                if client_request != "exit":
                    data = "completed"
                else:
                    data = "exit"
                conn.send(data.encode())

            except Exception as e:
                print("An error occurred:", str(e))
                error = str(e)
                conn.send(error.encode())

    except Exception as e:
        print("An error occurred outside the loop:", str(e))
    mongo.close_mongoDB()


if __name__ == "__main__":
    server_program()
