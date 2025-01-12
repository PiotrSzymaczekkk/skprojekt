#include <iostream>
#include <vector>
#include <string>
#include <sys/epoll.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <errno.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>
#include <poll.h>
#include <thread>
#include <queue>
#include <arpa/inet.h>
int c_fd;

void menu() {
    std::cout << "1 - stwórz nowy topic" << std::endl;
    std::cout << "2 <numer topicu> - zasubskrybuj topic" << std::endl;
    std::cout << "3 <numer topicu> <wiadomość> - wyślij wiadomość" << std::endl;
    std::cout << "4 - wyświetl twoje subskrypcje" << std::endl;
    std::cout << "5 - wyświetlić stworzone kolejki" << std::endl;
}

int brokerConnect(char* ip, char* port){
    c_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (c_fd < 0) {
        perror("socket");
        return 1;
    }

    sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr)); // Wyzerowanie struktury
    server_addr.sin_family = AF_INET;             // IPv4
    server_addr.sin_port = htons(atoi(port));  // Port serwera
    server_addr.sin_addr.s_addr = inet_addr(ip); // Adres IP serwera

    if (connect(c_fd, (sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("connect");
        close(c_fd);
        return 1;
    }
    std::cout << "Witaj kliencie!" << std::endl;
    std::cout << "Połączyłeś się z serwerem." << std::endl;
    std::cout << "Oto operacje, które możesz wykonać:" << std::endl;
    return c_fd;
}


// Funkcja tworząca kolejkę, zwracająca wskaźnik na stały ciąg znaków
void brokerCreateQ(std::string command) {
    if(command.size() == 1)
    write(c_fd, "1", 1);
    else std::cout << "Chcesz stworzyć kolejke? Wpisz \"1\"" << std::endl;
}

// Funkcja subskrybująca temat, zwracająca ciąg w postaci "2 <topic>"
void brokerSub(std::string command) {
    if (command.length() > 2) {
        if(command.substr(1,1) != " "){
            std::cout << "Zły format komendy" << std::endl;
            return;
        }
        size_t secondSpacePos = command.find(' ', 2);
        if (secondSpacePos != std::string::npos) {
            command = command.substr(0, secondSpacePos); // Zachowujemy tylko fragment do drugiej spacji
        }
        int size = command.size();
        for(int i = 2; i < size; i++){
            if(!std::isdigit(command[i])) {
                std::cout << "Nieprawidłowy format numeru topicu" << std::endl;
                return;
            }
        }
    } else {
        if(command.substr(1,0) != " ") menu();
        std::cout << "Podaj numer topicu" << std::endl;
        return;
    }
    write(c_fd, command.c_str(), command.size());
}

// Funkcja dodająca wiadomość do tematu, zwracająca ciąg w postaci "<topic> <wiadomosc>"
void brokerAddMsg(std::string command) {
    if (command.length() > 2 && command.substr(1,1) == " ") {
        size_t secondSpacePos = command.find(' ', 2);
        if (secondSpacePos == std::string::npos || command.size() - 1 == secondSpacePos) {
            std::cout << "Brak treści wiadomości" << std::endl;
            return;
        }
        int it = secondSpacePos;
        for(int i = 2; i < it; i++){
            if(!std::isdigit(command[i])) {
                std::cout << "Nieprawidłowy numer topicu" << std::endl;
                return;
            }
        }
        write(c_fd, command.c_str(), command.size());
    } else menu();
}

void brokerDispSubs(){
    write(c_fd, "4", 1);
}

void brokerDispTopics(){
    write(c_fd, "5", 1);
}