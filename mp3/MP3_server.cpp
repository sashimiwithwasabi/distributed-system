//This is the code for the server node (3-7).
#include <cstring>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <mutex>
#include <netinet/in.h>
#include <netdb.h>
#include <pthread.h>
#include <queue>
#include <signal.h>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <ctime>

using namespace std;        
namespace patch
{
    template<typename T> std::string to_string(const T&n)
    {
        std::ostringstream stm;
        stm<<n;
        return stm.str();
    }
}

/*  global variable definition  */
string version_code = "s-0.4.1: deadlock false positive";
int sigErrorPrint;  // count number of sig error printed
int localTest = 2;  //default mode is test on vm
int serverNum = 5;  // number of vms
int clientNum = 3;
int vmIndex; // index of this vm (start from 0) 0-2 client, 3-7 servers, 8 coordinator
int serverRange = 50;
int portBase = 4000; // can be modified by user
int buffer_size = 512; // socket message buffer size
int server_recv_socket[10]; // server sockets
int client_send_socket[10]; // client sockets
int verbose = 0;
int mssgLengths[10]; // message length array
bool vm_up[10]; // whether the vm is up (passively detect)
int vmNum = 10;

int opBuffer[3]; // 1 SET; 2 GET
string keyBuffer[3];
string valueBuffer[3];

mutex mtx_send_array[10];
mutex mtx_process_buffer;

unordered_map<string, string> storageMap;
unordered_map<string, string> tmpMap[3];
unordered_map<string, int> lockMap;

void* initializeServer(void*);
void* acceptConnection(void* param);
int processBuffer(string buffer, int serverIdx);
void* initializeClient(void*);
void* buildConnection(void* param);
void* commandInput(void*);
void* SendMessage(int receiver, int type, string key, string value);
void setOperation(int serverIdx, string key, string value);
void getOperation(int serverIdx, string key);

// client_node to server_node: 0 SET; 1 GET; 2 COMMIT; 3 ABORT
// svr_node to clnt_node: 10 SET OK; 11 GET OK; 12 COMMITED 13 GET NOT FOUND

/*  Catch Signal Handler function   */
void signal_callback_handler(int signum) {
    if (sigErrorPrint < 3) {
        fprintf(stderr,"[ERROR] sigpipe error!\n");
        sigErrorPrint++;
    }
}

/*  main processes defination   */
int main(int argc, char *argv[])
{   
    if (argc < 1) 
    {
        fprintf(stderr,"[ERROR] not enough params provided\n");
        exit(1);
    }
    vmIndex = atoi(argv[1]);
    if (argc >= 3) 
        localTest = atoi(argv[2]);  //localTest = 1, localtest mode

    if (argc >= 4) verbose = atoi(argv[3]);

    // register SIGPIPE
    signal(SIGPIPE, signal_callback_handler);

    cout<<"Server node mode\n";

    for (int i = 0; i < 10; i++) {
        vm_up[i] = true;
    }

    pthread_t serverThread, clientThread, commandThread, coordinatorThread;

    // create server threads
    int rc = pthread_create(&serverThread, NULL, initializeServer, NULL);
    if (rc)
    {
        std::cerr << "[Error] unable to create server thread," << rc << std::endl;
        exit(-1);
    }
    sched_yield();
    sleep(5);

    //  create client threads
    rc = pthread_create(&clientThread, NULL, initializeClient, NULL);
    if (rc){
        std::cerr << "[Error] unable to create thread," << rc << std::endl;
        exit(-1);
    }
    
    //  create user interface threads
    rc = pthread_create(&commandThread, NULL, commandInput, NULL);
    if (rc){
        std::cerr << "[Error] unable to create thread," << rc << std::endl;
        exit(-1);
    }

    pthread_exit(NULL);
    return 0;
}

void* initializeServer(void*){
    pthread_t messageThreads[vmNum];
    long idxes[vmNum];
        
    // when it is its turn to accept
    for (int j = 0; j <= 8; j++) {
        if (j >= 3 && j <= 7) continue;

        idxes[j] = j;
        int rc = pthread_create(&messageThreads[j], NULL, acceptConnection, (void *)idxes[j]);
        if (rc){
            std::cerr << "[ERROR] [server] unable to create thread," << rc << std::endl;
            exit(-1);
        }
    }
    
    pthread_exit(NULL);
}

void* acceptConnection(void* param){
    long serverIdx = (long)param;
    int my_socket = socket(PF_INET, SOCK_STREAM, 0);
    if(my_socket < 0) fprintf(stderr,"[ERROR] [serverAM %ld] Socket creation failed ...\n", serverIdx);
    struct sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port = htons(portBase + serverRange * vmIndex + serverIdx);
    bool flag = true;

    int yes = 1;
    if (setsockopt(my_socket, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) == -1) {
        fprintf(stderr,"[ERROR] [serverAM %ld] Trouble set sockopt ...\n", serverIdx);
        perror("setsockopt");
        exit(1);
    }
    
    int res = bind(my_socket, (struct sockaddr*) &address, sizeof(struct sockaddr_in));
    if(res< 0) {
        flag = false;
        fprintf(stderr,"[ERROR] [serverAM %ld] Trouble binding socket ...\n", serverIdx);
    }

    struct sockaddr_in remote_address;
    socklen_t remote_address_len;
    remote_address_len = sizeof(address);
    
    if (flag) {
        // reconnect
        res = listen(my_socket, 5);
        if(res < 0) {
            flag = false;
            fprintf(stderr,"[ERROR] [serverAM %ld] Couldn't listen to socket ...\n", serverIdx);
        }

        server_recv_socket[serverIdx] = accept(my_socket, (struct sockaddr*) &remote_address, &remote_address_len);
        
        if (verbose > 0)
            fprintf(stderr,"[LOG-1] [serverAM %ld] Server connected ...\n", serverIdx);

        vm_up[serverIdx] = true;

        // accept message
        char buffer[buffer_size];
        while (flag) {
            bzero(buffer,buffer_size);
            res = recv(server_recv_socket[serverIdx], buffer, buffer_size, 0);

            if (res != 0) {
                string recv_message_str = buffer;

                int type = processBuffer(recv_message_str, (int)serverIdx);

                // lengthy message
                if (type == 7) {
                    int mssg_length = mssgLengths[serverIdx];
                    string lengthyMssg = "";
                    mssgLengths[serverIdx] = 0;

                    if (verbose > 1)
                        fprintf(stdout,"[LOG-2] [%ld]I've received some long message: %d\n", serverIdx, mssg_length);

                    for (int i = 0; i < mssg_length; i++) {
                        bzero(buffer,buffer_size);
                        res = recv(server_recv_socket[serverIdx], buffer, buffer_size, 0);

                        // in case sender fail at middle
                        if (res <= 0) {
                            type = -1;
                            printf("[ERROR] sender fails at middle\n"); 
                            break;
                        }

                        string part_message_str = buffer;
                        lengthyMssg += part_message_str;
                    }

                    if (type == 7) {
                        processBuffer(lengthyMssg, (int)serverIdx);
                    }
                }

                if (type < 0) flag = false;
            } else {
                flag = false;
            }
        }      
    }

    shutdown(my_socket, 2);
    close(server_recv_socket[serverIdx]);
    close(my_socket);
    pthread_exit(NULL);
}

void setOperation(int serverIdx, string key, string value) {
    bool write_permission = true;
    if (lockMap.find(key) == lockMap.end()) {
        int new_lock = 1 << (serverIdx + 3);
        lockMap.insert(std::make_pair(key,new_lock));

        write_permission = true;
    } else {
        int old_lock = lockMap.find(key)->second;

        if ((old_lock & (1 << (serverIdx + 3))) != 0) {
            write_permission = true;
        } else if (old_lock > 7) {
            int bitMask = 0;
            for (int i = 0; i < 3; i++) {
                if ((old_lock & (1 << (i + 3))) != 0) {
                    bitMask |= (1 << i);
                }
            }
            SendMessage(8, 20, patch::to_string(serverIdx), patch::to_string(bitMask));

            write_permission = false;
        } else {
            int bitMask = 0;
            for (int i = 0; i < 3; i++) {
                if (i == serverIdx) continue;

                if ((old_lock & (1 << i)) != 0) {
                    write_permission = false;
                    bitMask |= (1 << i);
                }
            }

            if (!write_permission)
                SendMessage(8, 20, patch::to_string(serverIdx), patch::to_string(bitMask));
        }

        if (write_permission) {
            lockMap.find(key)->second |= (1 << (serverIdx + 3));
        }
    }

    if (write_permission) {
        if (tmpMap[serverIdx].find(key) == tmpMap[serverIdx].end())
            tmpMap[serverIdx].insert(std::make_pair(key,value));
        else
            tmpMap[serverIdx].find(key)->second = value;

        SendMessage(serverIdx, 10, "", "");
    } else {
        // add to buffer
        opBuffer[serverIdx] = 1;
        keyBuffer[serverIdx] = key;
        valueBuffer[serverIdx] = value;
    }
}

void getOperation(int serverIdx, string key) {
    bool read_permission = true;
    if (lockMap.find(key) == lockMap.end()) {
        int new_lock = 1 << (serverIdx);
        lockMap.insert(std::make_pair(key,new_lock));

        read_permission = true;
    } else {
        int old_lock = lockMap.find(key)->second;

        if ((old_lock & (1 << (serverIdx + 3))) != 0) {
            read_permission = true;
        } else if (old_lock > 7) {
            int bitMask = 0;
            for (int i = 0; i < 3; i++) {
                if (i == serverIdx) continue;

                if ((old_lock & (1 << (i + 3))) != 0) {
                    bitMask |= (1 << i);
                }
            }
            SendMessage(8, 20, patch::to_string(serverIdx), patch::to_string(bitMask));

            read_permission = false;
        } else {
            read_permission = true; // shared lock
        }

        if (read_permission) {
            lockMap.find(key)->second |= (1 << serverIdx);
        }
    }

    if (read_permission) {
        if (tmpMap[serverIdx].find(key) != tmpMap[serverIdx].end()) {
            string value = tmpMap[serverIdx].find(key)->second;
            SendMessage(serverIdx, 11, key, value);
        } else if (storageMap.find(key) != storageMap.end()) {
            string value = storageMap.find(key)->second;
            SendMessage(serverIdx, 11, key, value);
        } else
            SendMessage(serverIdx, 13, "", "");
    } else {
        // add to buffer
        opBuffer[serverIdx] = 2;
        keyBuffer[serverIdx] = key;
    }
}

int processBuffer(string recv_message_str, int serverIdx){
    mtx_process_buffer.lock();

    if (recv_message_str.length() < 1) {
        // what is this? E.T.?
        printf("[ERROR] E.T.?\n");

        mtx_process_buffer.unlock();
        return -1;
    }

    int pos_mode = recv_message_str.find(" ");
    if (pos_mode == std::string::npos || pos_mode == 0) {
        printf("[ERROR] Strange message?\n");

        mtx_process_buffer.unlock();
        return -2;
    }

    int mode = stoi(recv_message_str.substr(0, pos_mode));

    if(mode == 0) { // mode 0 : SET
        // key
        int pos_key = recv_message_str.find(" ", pos_mode + 1);
        int key_size = stoi(recv_message_str.substr(pos_mode + 1, pos_key - pos_mode - 1));
        string key = recv_message_str.substr(pos_key + 1, key_size);

        // value
        int pos_value = recv_message_str.find(" ", pos_key + 1 + key_size + 1);
        int value_size = stoi(recv_message_str.substr(pos_key + 1 + key_size + 1, pos_value - pos_key - 1));
        string value = recv_message_str.substr(pos_value + 1, value_size);

        setOperation(serverIdx, key, value);
    } 
    else if(mode == 1)  // mode 1 : GET
    {
        // key
        int pos_key = recv_message_str.find(" ", pos_mode + 1);
        int key_size = stoi(recv_message_str.substr(pos_mode + 1, pos_key - pos_mode - 1));
        string key = recv_message_str.substr(pos_key + 1, key_size);

        getOperation(serverIdx, key);
    }
    else if(mode == 2)  // mode 2: COMMIT
    {
        cout<<"COMMIT! Cool!\n";

        // store tmp data
        for (auto it = tmpMap[serverIdx].begin(); it != tmpMap[serverIdx].end(); ++it) {
            string key = it->first;
            if (storageMap.find(key) == storageMap.end())
                storageMap.insert(std::make_pair(key,it->second));
            else
                storageMap.find(key)->second = it->second;
        }

        tmpMap[serverIdx].erase(tmpMap[serverIdx].begin(), tmpMap[serverIdx].end());

        // clear locks
        for (auto it = lockMap.begin(); it != lockMap.end(); ++it) {
            int bitmask = 1 << serverIdx; // read
            bitmask |= 1 << (serverIdx + 3); // write
            bitmask = ~bitmask;

            it->second &= bitmask;
        }

        SendMessage(serverIdx, 12, "", "");
    }
    else if(mode == 3)  //  mode 3: ABORT
    {
        cout<<"ABORT! Well~\n";

        tmpMap[serverIdx].erase(tmpMap[serverIdx].begin(), tmpMap[serverIdx].end());

        // clear locks
        for (auto it = lockMap.begin(); it != lockMap.end(); ++it) {
            int bitmask = 1 << serverIdx; // read
            bitmask |= 1 << (serverIdx + 3); // write
            bitmask = ~bitmask;

            it->second &= bitmask;
        }

        SendMessage(serverIdx, 12, "", "");
    }
    else if (mode == 7) 
    {
        // get length
        int pos_mssg_length = recv_message_str.find(" ", pos_mode + 1);
        int mssg_length = stoi(recv_message_str.substr(pos_mode + 1, pos_mssg_length - pos_mode - 1));

        // store length into lengths array
        mssgLengths[serverIdx] = mssg_length;
    }
    else if (mode == 25)
    {
        // node
        int new_node = stoi(recv_message_str.substr(pos_mode + 1));

        int last_op = opBuffer[new_node];
        if (last_op == 1) { // SET
            setOperation(new_node, keyBuffer[new_node], valueBuffer[new_node]);
        } else { // GET
            getOperation(new_node, keyBuffer[new_node]);
        }
    }

    mtx_process_buffer.unlock();

    if (mode >= 0) return mode;
    else return -1;
}

void* initializeClient(void*){
    pthread_t messageThreads[vmNum];
    long idxes[vmNum];
        
    // when it is its turn to connect
    for (int j = 0; j <= 8; j++) {
        if (j >= 3 && j <= 7) continue;

        idxes[j] = j;
        int rc = pthread_create(&messageThreads[j], NULL, buildConnection, (void *)idxes[j]);
        if (rc){
            std::cerr << "[ERROR] [server] unable to create thread," << rc << std::endl;
            exit(-1);
        }
    }
    
    pthread_exit(NULL);
}

void* buildConnection(void* param) {
    long serverIdx = (long)param;
    bool flag = true;

    client_send_socket[serverIdx]=socket(PF_INET, SOCK_STREAM, 0);
    if(client_send_socket[serverIdx] < 0) {
        flag = false;
        fprintf(stderr,"[ERROR] [client] Socket %ld creation failed in client...\n", serverIdx);
    }
        
    struct sockaddr_in address;
    address.sin_family = AF_INET;
        
    // set server address
    if (localTest == 2) { // vm test
        struct hostent *server;
        string vm=patch::to_string(serverIdx + 1);
        string server_name;
        if (serverIdx == 9) server_name = "sp17-cs425-g17-10.cs.illinois.edu";
        else server_name="sp17-cs425-g17-0" + vm + ".cs.illinois.edu";
            
        char *server_name_char = (char *)alloca(server_name.size() + 1);
        memcpy(server_name_char, server_name.c_str(), server_name.size() + 1);
        server = gethostbyname(server_name_char);
        if (server == NULL) {
            fprintf(stderr,"[ERROR] no such host\n");
            exit(0);
        }
        bcopy((char *)server->h_addr,
            (char *)&address.sin_addr.s_addr,
            server->h_length);
    } else { // local test
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    }
        
    address.sin_port = htons(portBase + serverRange * serverIdx + vmIndex);

    while (flag) {
        while (!vm_up[serverIdx]) {
            usleep(1000);
            continue;
        }

        client_send_socket[serverIdx]=socket(PF_INET, SOCK_STREAM, 0);
        int res = connect(client_send_socket[serverIdx], (struct sockaddr*) &address, sizeof(struct sockaddr));
        if (res < 0) {
            
        } else {
            if (verbose > 0)
                fprintf(stderr,"[LOG-1] [clientAM %ld] Server connected ...\n", serverIdx);

            while (res >= 0 && client_send_socket[serverIdx] > 0) {
                sleep(1);
            }
        }

        if (verbose > 0)
            fprintf(stderr,"[LOG-1] [clientAM %ld] Server disconnected ...\n", serverIdx);

        vm_up[serverIdx] = false;
    }

    pthread_exit(NULL);
}

//  commandInput = user interface thread
void* commandInput(void*){
    bool flag = true;
    while(flag)
    {
        string commandStr;
        getline(cin,commandStr);
        
        if (commandStr[0] == 'a')
        {
            cerr<<"print all pairs:"<<endl;
            for ( auto it = storageMap.begin(); it != storageMap.end(); ++it )
                std::cout << "key: " << it->first << " value:" << it->second<<endl;
        } else if (commandStr[0] == 'v')
        {
            cerr<<version_code<<endl;
        }

        usleep(1000);
    }
    pthread_exit(NULL);
}

void* SendMessage(int receiver, int type, string key, string value){
    stringstream sendStrSS;
    switch(type)
    {
        case 10: // mode 10 : SET OK
            sendStrSS << 10 << ' ';
            break;
        case 11: //  mode 11: GET OK
            sendStrSS << 11 << ' ' << key.length() << ' ' << key << ' ' << value.length() << ' ' << value;
            break;
        case 12: //  mode 12: COMMIT/ABORT done
            sendStrSS << 12 << ' ';
            break;
        case 13: //  mode 13: GET not found
            sendStrSS << 13 << ' ';
            break;
        case 20: //  mode 20: register lock
            sendStrSS << 20 << ' ' << key << ' ' << value;
            break;
        default:
            break;
    }

    // partition string if too long
    string sendStr = sendStrSS.str();
    if (sendStr.length() > buffer_size - 10) {

        int parts = (sendStr.length() - 1) / (buffer_size - 10) + 1;

        // send length message first
        stringstream lengthStrSS;
        lengthStrSS << 7 << ' ' << parts << ' ';

        char message_buffer[buffer_size];
        bzero(message_buffer, buffer_size);
        strcpy(message_buffer, lengthStrSS.str().c_str());

        mtx_send_array[receiver].lock();

        int res = send(client_send_socket[receiver], message_buffer, buffer_size, 0);
        if(res < 0) cerr << "[ERROR] send (length message)" << endl;

        // send message part by part
        for (int i = 0; i < parts - 1; i++) {
            bzero(message_buffer, buffer_size);
            string partStr = sendStr.substr(i * (buffer_size - 10), (buffer_size - 10));

            if (verbose > 1)
                printf("[LOG-2] partStr[%d] : %s\n", i, partStr.c_str());

            strcpy(message_buffer, partStr.c_str());

            res = send(client_send_socket[receiver], message_buffer, buffer_size, 0);
            if(res < 0) cerr << "[ERROR] send (parts message)" << endl;

            usleep(1000); // v1
        }

        bzero(message_buffer, buffer_size);
        strcpy(message_buffer, sendStr.substr((parts - 1) * (buffer_size - 10)).c_str());

        if (verbose > 1)
            printf("[LOG-2] partStr[last] : %s\n", sendStr.substr((parts - 1) * (buffer_size - 10)).c_str());

        res = send(client_send_socket[receiver], message_buffer, buffer_size, 0);
        if(res < 0) cerr << "[ERROR] send (last part)" << endl;

        mtx_send_array[receiver].unlock();
    } else {
        char message_buffer[buffer_size];
        bzero(message_buffer, buffer_size);
        strcpy(message_buffer, sendStrSS.str().c_str());

        mtx_send_array[receiver].lock();

        int res = send(client_send_socket[receiver], message_buffer, buffer_size, 0);
        if(res < 0) cerr << "[ERROR] send (single message)" << endl;

        mtx_send_array[receiver].unlock();
    }
}