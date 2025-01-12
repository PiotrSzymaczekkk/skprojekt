#include <iostream>
#include <vector>
#include <string>
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
#include <mutex>
#include <condition_variable>
int BUFF_SIZE = 255;

class Broker { 
    private:
        int ttl = 5;
        int s_fd;
        struct message {
            std::chrono::steady_clock::time_point ttl; //czas życia
            std::string msg; //wiadomość
            std::vector<int> receivers;
        };
        struct subscryption {
            int c_id; //kto
            bool current; //flaga
        };
        std::vector<std::vector<subscryption>> subs;
        std::vector<int> topics;
        std::vector<int> clients;
        std::queue<message> failedMessages;
        std::mutex failedMessagesMutex;
        std::condition_variable failedMessagesCv;
    public:
        void handleFailedMessages() {
            while (true) {
                std::unique_lock<std::mutex> lock(failedMessagesMutex);
                failedMessagesCv.wait(lock, [this]() { return !failedMessages.empty(); });
                while (!failedMessages.empty()) {
                    message msg = failedMessages.front();
                    failedMessages.pop();
                    lock.unlock();
                    if (std::chrono::steady_clock::now() > msg.ttl) {
                        lock.lock();
                        continue;
                    }
                    for (auto it = msg.receivers.begin(); it != msg.receivers.end();) {
                        if (sendMsg(*it, msg.msg) == -1) {
                            lock.lock();
                            failedMessages.push(msg);
                            lock.unlock();
                            std::this_thread::sleep_for(std::chrono::seconds(1));
                            return;
                        } else {
                            it = msg.receivers.erase(it);
                        }
                    }
                    lock.lock();
                }
            }
        }

        void setTTL(int x){
            ttl = x;
        }
        int createServerSock(char* port){ //tested
            sockaddr_in serwer{};
            serwer.sin_family=AF_INET;
            serwer.sin_addr={htonl(INADDR_ANY)};
            serwer.sin_port=htons(atoi(port));
            int s_fd=socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
            if(s_fd == -1){
                perror("socket failed");
            }
            if(bind(s_fd, (sockaddr*) &serwer, sizeof(serwer)) == -1){
                perror("bind failed");
            }
            if(listen(s_fd,1) == -1){
                perror("listen failed");
            }
            return s_fd;
        }
        int addClient(int epollFd){ //tested
            int c_fd = accept(s_fd, NULL, NULL);
            if(c_fd == -1){
                perror("accept error");
                return -1;
            }
            epoll_event ee;   
            ee.events = EPOLLIN;
            ee.data.fd = c_fd;
            if (epoll_ctl(epollFd, EPOLL_CTL_ADD, c_fd, &ee) == -1) {
                    perror("epoll_ctl");
                    close(c_fd);
                    return -1;
            }
            bool _new = 1;
            int size = clients.size();
            for(int i = 0; i < size; i++){
                if(clients[i] == -1) {
                    clients[i] = c_fd;
                    _new = 0;
                }
            }
            if(_new == 1) clients.push_back(c_fd);
            std::cout << "Nowe połączenie od klienta: " << c_fd << std::endl;
            return 0;
        }
        int readCommand(int c_fd, std::string *command){ //tested
            char tmpBuffer[32] = {};
            ssize_t bytes_read = read(c_fd, tmpBuffer, sizeof(tmpBuffer) - 1);
            if (bytes_read <= 0) {
                if (bytes_read == 0) {
                } else {
                    perror("read error");
                }
                return -1; // Informacja dla serwera o rozłączeniu klienta
            }
            std::string tmpStr(tmpBuffer, bytes_read);
            int size = tmpStr.size();
            // std::cout << size << std::endl;
            int type = atoi((char*)tmpStr.substr(0,1).c_str());
            std::cout << type << std::endl;
            if(size > 1){
                std::string text = tmpStr.substr(2, size - 2);
                *command = text;
            }
            return type;
        }
        int createQ(){ //tested
            std::vector<subscryption> sub;
            subs.push_back(sub);
            topics.push_back(topics.size());
            int t_id = topics.size()-1;
            return t_id;
        }
        int addSub(int clientFD, std::string command){ //tested
            int topicID;
            try {
                topicID = std::stoi(command) - 1;
            } catch (const std::exception &e) {
                sendMsg(clientFD, "Subscription command format error");
                return 0;
            }
            int subs_size = subs.size();
            if (topicID < 0 || topicID >= subs_size) {
                sendMsg(clientFD, "Nie ma takiego topicu");
                return 0;
            }
            int topic_sub_size = subs[topicID].size();
            for(int i = 0; i < topic_sub_size; i++){
                if(subs[topicID][i].c_id == clientFD && subs[topicID][i].current == 1){
                    subs[topicID][i].current = 0;
                    return -(topicID+1);
                }
                if(subs[topicID][i].c_id == clientFD && subs[topicID][i].current == 0){
                    subs[topicID][i].current = 1;
                    return (topicID+1);
                }
            }
            for(int i = 0; i < topic_sub_size; i++){
                if(subs[topicID][i].current == 0){
                    subs[topicID][i].c_id = clientFD;
                    subs[topicID][i].current = 1;
                    return topicID+1;
                }
            }
            subscryption sub;
            sub.c_id = clientFD;
            sub.current = 1;
            subs[topicID].push_back(sub);
            return topicID+1;
        }
        void addMsg(int clientFD, std::string command){ //tested
            try {
                size_t spacePos = command.find(' ');
                int topicID = std::stoi(command.substr(0, spacePos).c_str()) - 1;
                int topics_size = topics.size();
                if (topicID < 0 || topicID >= topics_size) {
                    sendMsg(clientFD, "Nie ma takiego topicu");
                    return;
                }
                message msg;
                msg.ttl = std::chrono::steady_clock::now() + std::chrono::seconds(ttl);
                for(subscryption i : subs[topicID]){
                    if(i.current == 1){
                        if(sendMsg(i.c_id, "Nowa wiadomość w topicu nr " + std::to_string(topicID + 1) + " : " + command.substr(spacePos + 1)) == -1){
                            msg.receivers.push_back(i.c_id);
                        }
                    }
                }
                if(msg.receivers.size() > 0){
                    msg.msg = "Nowa wiadomość w topicu nr " + std::to_string(topicID + 1) + " : " + command.substr(spacePos + 1);
                    {
                    std::unique_lock<std::mutex> lock(failedMessagesMutex);
                    failedMessages.push(msg);
                    }
                    failedMessagesCv.notify_one();
                    std::cout << "jest" << std::endl;
                }
                return;
            } catch (const std::exception &e) {
                sendMsg(clientFD, "Błędny format komendy");
                return;
            }
        }
        int sendMsg(int fd, std::string msg){ //tested
            if(fd == 0){
                int clients_size = clients.size();
                for(int i = 0; i < clients_size; i++){
                    if(clients[i] != -1)
                        sendMsg(clients[i], msg);
                }
                return 0;
            } else {
                int size = msg.size();
                std::cout << "sending to " << fd << std::endl;
                if(write(fd, msg.c_str(), size) < 0) return -1;
                return 0;
            }
        }
        void viewSubscryptionList(int c_fd) { //tested
            sendMsg(c_fd, "Lista twoich subskrypcji:\n");
            bool anySubs = 0;
            int sub_size = subs.size();
            for(int i = 0; i < sub_size; i++){
                int topic_sub_size = subs[i].size();
                for(int j = 0; j < topic_sub_size; j++){
                    if(subs[i][j].c_id == c_fd && subs[i][j].current == 1){
                        anySubs = 1;
                        sendMsg(c_fd, std::to_string(i+1) + ", ");
                    }
                }
            }
            if(anySubs == 0) sendMsg(c_fd, "Nie masz żadnych subskrypcji :C");
        }
        void viewTopicsList(int c_fd){ //tested
            int size = topics.size();
            if(size == 0) sendMsg(c_fd, "Nie ma żadnych topiców");
            else if(size != 1) sendMsg(c_fd, "Istnieją topici od 1 do " + std::to_string(topics.size()));
            else sendMsg(c_fd, "Istnieje tylko topic 1");
        }
        void deleteClient(int clientFD, int epollFD){
            close(clientFD);
            epoll_ctl(epollFD, EPOLL_CTL_DEL, clientFD, nullptr);
            int subs_size = subs.size();
            for(int i = 0; i < subs_size; i++){
                int topic_sub_size = subs[i].size();
                std::cout << i << ", ";
                for(int j = 0; j < topic_sub_size; j++){
                    std::cout << j << std::endl;
                    if(subs[i][j].c_id == clientFD) subs[i][j].current = 0;
                }
            }
            std::cout << "closing: " << clientFD << std::endl;
            int clients_size = clients.size();
            for(int i = 0; i < clients_size; i++){
                if(clients[i] == clientFD){
                clients[i] = -1;
                break;
                }
            }
        }
        int run(char *port, int ttl){ //tested
            Broker::ttl = ttl;
            Broker::s_fd = createServerSock(port);
            int epoll_fd = epoll_create1(0);
            epoll_event ee;
            ee.events = EPOLLIN;
            ee.data.fd = s_fd;
            epoll_ctl(epoll_fd, EPOLL_CTL_ADD, s_fd, &ee);
            std::thread(&Broker::handleFailedMessages, this).detach();
            while(1){
                if(epoll_wait(epoll_fd, &ee, 1, -1) == -1){
                    perror("epoll_wait failed");
                }
                if(ee.data.fd == s_fd){
                    addClient(epoll_fd);
                } else {
                    int c_fd = ee.data.fd;
                    std::string command;
                    int type = readCommand(c_fd, &command);
                    if (type == -1) {
                        std::cout << "po readCommand" << std::endl;
                        deleteClient(c_fd, epoll_fd);
                        continue;
                    }
                    switch (type) {
                        case 1: //tworzenie kolejki
                            sendMsg(0, "Utworzono kolejkę o id: " + std::to_string(createQ() + 1));
                            break;
                        case 2: //subskrypcja
                            int topicID;
                            if((topicID = addSub(c_fd, command)) > 0){
                                sendMsg(c_fd, "Pomyślnie zasubskrybowano topic " + std::to_string(topicID));
                            } else if(topicID < 0) sendMsg(c_fd, "Pomyślnie odsubskrybowano topic " + std::to_string(-topicID));
                            break;
                        case 3:
                            addMsg(c_fd, command);
                            break;
                        case 4:
                            viewSubscryptionList(c_fd);
                            break;
                        case 5:
                            viewTopicsList(c_fd);
                            break;
                        default:
                            break;
                    }
                }
            }
        }
};

struct command{
    int type;
    char* msg;
};

int main(int argc, char* argv[])
{
    int ttl;
    if(argc!=3){
        std::cout << "Usage: " << argv[0]  << " <port> <ttl>" << std::endl;
        return 1;
    } else if((ttl = std::stoi(argv[2])) < 0){
        std::cout << "Zły format ttl" << std::endl;
        return 1;
    }
    Broker server;
    server.run(argv[1], ttl);
    return 0;
}
