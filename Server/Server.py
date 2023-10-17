import socket
import json
from Controller import Controller


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

        data = ""

        while data != "exit":
            try:  # Add a try block inside the while loop
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
                elif command_type == "drop" and instance_type == "database":
                    controller.drop_database()

                elif command_type == "create" and instance_type == "table":   # create table table_name (...) on database_name
                    controller.create_table(client_request)

                elif command_type == "drop" and instance_type == "table":    # drop table table_name on database
                    db_name = commands[4].upper()
                    controller.delete_table(db_name)

                elif command_type == "create" and instance_type == "index":   # create index index_name (table_name,column_name) on database_name
                    controller.create_index(client_request)

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


if __name__ == "__main__":
    server_program()
