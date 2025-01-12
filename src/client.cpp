#include "lib.hpp"
#include <iostream>
#include <arpa/inet.h>
#include <vector>
#include <sys/epoll.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <errno.h>
#include <netdb.h>
#include <string>
#include <sys/socket.h>
#include <unistd.h>
#include <poll.h>
#include <thread>
#include <queue>

void receive_messages(int socket_fd) {
    char buffer[1024];
    while (true) {
        memset(buffer, 0, sizeof(buffer));
        ssize_t bytes_received = read(socket_fd, buffer, sizeof(buffer) - 1);
        if (bytes_received < 0) {
            perror("read");
            break;
        } else if (bytes_received == 0) {
            std::cout << "Serwer zakończył połączenie." << std::endl;
            break;
        }
        std::cout << buffer << std::endl;
    }
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Użycie: " << argv[0] << " <ip serwera> <port serwera>" << std::endl;
        return 1;
    }
    int c_fd = brokerConnect(argv[1], argv[2]);
    menu();
    std::thread receiver_thread(receive_messages, c_fd); //odbieranie wiadomości
    std::string buffer;
    while (1) { // wysyłanie wiadomości
        std::getline(std::cin, buffer);  // Używamy getline, aby pobrać całą linię
        if (buffer == "menu") {
            menu();
            continue;
        }
        if (buffer.length() > 0) {
            if(!std::isdigit(buffer[0])) {
                std::cout << "Komendy zaczynają się od cyfry!!!" << std::endl;
                continue;
            }
            int type = std::stoi(buffer.substr(0, 1)); // Konwertowanie pierwszego znaku na liczbę
            buffer = buffer.c_str();

            switch (type) {
                case 1:
                        brokerCreateQ(buffer);
                        break;
                case 2:
                        brokerSub(buffer);
                        break;
                case 3:
                        brokerAddMsg(buffer);
                        break;
                case 4:
                        brokerDispSubs();
                        break;
                case 5:
                        brokerDispTopics();
                        break;
                default:
                    std::cout << "Nieznana komenda!" << std::endl;
                    break;
            }
        }
    }
}