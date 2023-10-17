from InputManipulator import InputManipulator
import socket
import json


def client_program():
    try:
        host = socket.gethostname()
        port = 5235

        client_socket = socket.socket()
        client_socket.connect((host, port))
        user_input = ""

        while user_input != "exit":
            try:
                user_input = input("\nEnter the command: ")
                if user_input != "exit":
                    input_manipulator = InputManipulator(user_input)
                    input_manipulator.validate_input()

                client_socket.send(user_input.encode())
                server_response = client_socket.recv(1024).decode()
                print("\nServer says:", server_response)

            except Exception as e:
                print("\nAn error occurred:", str(e))

        client_socket.close()
    except Exception as e:
        print("\nAn error occurred:", str(e))


if __name__ == '__main__':
    client_program()

