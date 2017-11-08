#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <vector>
#include <utility>
#include <set>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <iostream>
#include <cstdlib>
#include <pthread.h>
#include <cstring>
#include <sstream>
#include <signal.h>
#include <unordered_map>
#include <mutex>
using namespace std;

// 1.4.6 with heartbeat, don't check server setup
// code clear

namespace patch {
    template<typename T> std::string to_string(const T&n) {
        std::ostringstream stm;
        stm<<n;
        return stm.str();
    }
}

struct status {
    int mode = -1; // 0-2: ISIS; 3: ; 4: heartbeat;
    string message_str = ""; // message content
    int message_id = 0; // number of index in sender messages
    int seq_num = 0; // message sequence number
    int sender_num = 0; // sender's vm index
    int proposer_id = 0; // the vm index of finilized sequence number
    bool deliverable = false; // whether this message is deliverable
};

void* do_server(void*);
void* do_client(void*);
void* acceptMessage(void*);
void* SendMessage(string send_text,int input_mode,int input_message_id,pair<int,int> pa);
void* recv_heartbeat(void* param);
void* send_heartbeat(void* param);
string serialize_status(status from_status);
status deserialize_string(string from_string);
void* message_delivery(void* param);

void* hb_do_server(void*);
void* hb_do_client(void*);
void* hb_acceptMessage(void*);
void* send_heartbeat_thread(void* param);

void error(const char *msg) {
    perror(msg);
    exit(1);
}

mutex mtx;           // mutex for critical section

int sigErrorPrint; // count number of sig error printed
int vmNum; // number of vms
int vmIndex; // index of this vm (start from 0)
int serverRange = 50;
int portBase = 3000; // can be modified by user
int max_delay_ms = 100; // max delay of message (in ms)
int aliveServer; // number of alive server
int localTest; // 1 for local, 2 for vm
int server_recv_socket[10] = {0}; // server sockets
int server_setup_socket[10];
int client_send_socket[10]; // client sockets
int node_message_left[10]; // number of message left for a failed node
int hb_server_recv_socket[10] = {0}; // hb_server sockets
int hb_client_send_socket[10]; // hb_client sockets
bool client_heartbeat_received[10]; // heartbeat is received from that client
int buffer_size =1024; // socket message buffer size
int total_message_id = 0; // allocated for each vm, used in client when sending
int total_sequence_num = 0;//allocated for each vm,used in server when receving
bool self_alive = true; // whether node itself is alive
int abadon_node_mask = 0; // bitmask of abandoned nodes
int alive_node_mask = 0; // bitmask of alive nodes
int lazy = 50; // heartbeat detection time
string version_code = "1.4.6";
unordered_map<int, int> proposed_sequence_bitmask;

unordered_map<int,pair<int,int> > sent_seq_proposer; // this used to store the flags of the send-back message numbers. index=mid;

class Comp_Operator {
public:
    bool operator()(status const & s1,status const & s2) { 
         return (s1.seq_num < s2.seq_num || (s1.seq_num==s2.seq_num&&s1.proposer_id<s2.proposer_id));
    }
};

/* Catch Signal Handler functio */
void signal_callback_handler(int signum) {
    if (sigErrorPrint < 3) {
        printf("sigpipe error!\n");
        sigErrorPrint++;
    }
}

set<status,Comp_Operator> pq;//this is used for storing message to be sent

int main(int argc, char *argv[]) {
    if (argc < 5) {
        fprintf(stderr,"ERROR, not enough params provided\n");
        exit(1);
    }
    vmNum = atoi(argv[1]);
    vmIndex = atoi(argv[2]);
    localTest = atoi(argv[3]); // 1 for local, 2 for vm
    portBase = atoi(argv[4]);
    lazy = atoi(argv[5]);

    // register SIGPIPE
    signal(SIGPIPE, signal_callback_handler);
    
    pthread_t serverThread, clientThread, messageCenterThread;
    pthread_t hb_serverThread, hb_clientThread;

    // thread to setup server sockets
    int rc = pthread_create(&serverThread, NULL, do_server, NULL);
    if (rc){
        std::cout << "Error:unable to create thread," << rc << std::endl;
        exit(-1);
    }

    // thread to setup heartbeat(hb) server sockets
    rc = pthread_create(&hb_serverThread, NULL, hb_do_server, NULL);
    if (rc){
        std::cout << "Error:unable to create thread," << rc << std::endl;
        exit(-1);
    }

    // wait for some time to receive connection
    sched_yield();
    sleep(10);

    // thread to setup client sockets
    rc = pthread_create(&clientThread, NULL, do_client, NULL);
    if (rc){
        std::cout << "Error:unable to create thread," << rc << std::endl;
        exit(-1);
    }

    // thread to setup heartbeat client sockets
    rc = pthread_create(&hb_clientThread, NULL, hb_do_client, NULL);
    if (rc){
        std::cout << "Error:unable to create thread," << rc << std::endl;
        exit(-1);
    }

    // thread to print top message of priorityqueue
    rc = pthread_create(&messageCenterThread, NULL, message_delivery, NULL);
    if (rc){
        std::cout << "Error:unable to create thread," << rc << std::endl;
        exit(-1);
    }

    /*sleep(10);
    for (int i = 0; i < vmNum; i++) {
        if (i == vmIndex) continue;

        if (server_recv_socket[i] == 0) {
            shutdown(server_setup_socket[i], 2);
            close(server_setup_socket[i]);
            printf("Node %d not connected\n", i);
        }
    }*/
    
    pthread_exit(NULL);
    return 0;
}

void* do_server(void*) {
    aliveServer = vmNum - 1;
    pthread_t messageThreads[vmNum];
    long idxes[vmNum];
        
    for(int i = 0; i < vmNum; i++) {
        if (i != vmIndex) continue;

        // when it is its turn to accept
        for (int j = 0; j < vmNum; j++) {
            if (j == vmIndex) continue;
            idxes[j] = j;
            int rc = pthread_create(&messageThreads[j], NULL, acceptMessage, (void *)idxes[j]);
            if (rc){
                std::cout << "[server] Error:unable to create thread," << rc << std::endl;
                exit(-1);
            }
        }
    }
       
    //printf("[server] All sockets ready ...\n"); // verbose

    while (aliveServer > 0) ;
    
    pthread_exit(NULL);
}

void* hb_do_server(void*) {
    pthread_t messageThreads[vmNum];
    long idxes[vmNum];
        
    for(int i = 0; i < vmNum; i++) {
        if (i != vmIndex) continue;

        // when it is its turn to accept
        for (int j = 0; j < vmNum; j++) {
            if (j == vmIndex) continue;
            idxes[j] = j;
            int rc = pthread_create(&messageThreads[j], NULL, hb_acceptMessage, (void *)idxes[j]);
            if (rc){
                std::cout << "[server] Error:unable to create thread," << rc << std::endl;
                exit(-1);
            }
        }
    }
       
    //printf("[server] All HB sockets ready ...\n"); // verbose

    while (aliveServer > 0) ;
    
    pthread_exit(NULL);
}

void* hb_acceptMessage(void* param) {
    // set up server socket param
    long serverIdx = (long)param;
    int my_socket = socket(PF_INET, SOCK_STREAM, 0);
    if(my_socket < 0) printf("[serverAM %ld] Socket creation failed ...\n", serverIdx);
    struct sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port = htons(portBase + 500 + serverRange * vmIndex + serverIdx);
    bool flag = true;
    
    // bind socket with port, and wait for client to connect with
    int res = bind(my_socket, (struct sockaddr*) &address, sizeof(struct sockaddr_in));
    if(res< 0) {
        flag = false;
        printf("[serverAM %ld] Trouble binding socket ...\n", serverIdx);
    }
    
    if (flag) {
        res = listen(my_socket, 5);
        if(res < 0) {
            flag = false;
            printf("[serverAM %ld] Couldn't listen to socket ...\n", serverIdx);
        }

        struct sockaddr_in remote_address;
        socklen_t remote_address_len;
        remote_address_len = sizeof(address);

        //printf("[serverAM %ld] Accepting for socket ...\n", serverIdx); // verbose
        hb_server_recv_socket[serverIdx] = accept(my_socket, (struct sockaddr*) &remote_address, &remote_address_len);

        if (hb_server_recv_socket[serverIdx] < 0) {
            flag = false;
        } else {
            // create thread to check heartbeat
            pthread_t server_recv_heartbeat_t;
            res = pthread_create(&server_recv_heartbeat_t, NULL, recv_heartbeat, (void *)serverIdx);
            if (res < 0){
                std::cout << "[server] Error:unable to create thread," << res << std::endl;
                exit(-1);
            }
        }
    }

    // continously listen to arrived message
    while (flag) {
        //status buffer0;
        char recv_message_buffer[buffer_size];
        bzero(recv_message_buffer, buffer_size);
        int res = recv(hb_server_recv_socket[serverIdx], recv_message_buffer, buffer_size, 0); // serialize
        string recv_message_str = recv_message_buffer;
        status buffer0 = deserialize_string(recv_message_str);

        if (res > 0) {
            if (buffer0.mode == -1) {
                
            } else if (buffer0.mode == 4) {
                client_heartbeat_received[serverIdx] = true;
            }                
        } else {//res==-1   
            printf("[serverAM %ld] server shutdown...\n", serverIdx);

            // modify abandon_mask and alive_mask
            int bitmask = 1 << serverIdx;
            abadon_node_mask |= bitmask;
            bitmask = ~bitmask;
            alive_node_mask &= bitmask;
            flag = false;

            // clear socket
            shutdown(hb_client_send_socket[serverIdx], 2);
            close(hb_client_send_socket[serverIdx]);
            hb_client_send_socket[serverIdx] = -1;
        }     
    } // end of while(flag)

    // close this server and clear up
    shutdown(my_socket, 2);
    close(hb_server_recv_socket[serverIdx]);
    close(my_socket);
    //printf("[hb serverAM %ld] Exiting server code...\n", serverIdx); // verbose
    pthread_exit(NULL);
}

void* acceptMessage(void* param) {
    // set up server socket param
    long serverIdx = (long)param;
    int my_socket = socket(PF_INET, SOCK_STREAM, 0);
    if(my_socket < 0) printf("[serverAM %ld] Socket creation failed ...\n", serverIdx);
    server_setup_socket[serverIdx] = my_socket;
    struct sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port = htons(portBase + serverRange * vmIndex + serverIdx);
    bool flag = true;
    
    // bind socket with port, and wait for client to connect with
    int res = bind(my_socket, (struct sockaddr*) &address, sizeof(struct sockaddr_in));
    if(res< 0) {
        flag = false;
        printf("[serverAM %ld] Trouble binding socket ...\n", serverIdx);
    }
    
    if (flag) {
        res = listen(my_socket, 5);
        if(res < 0) {
            flag = false;
            printf("[serverAM %ld] Couldn't listen to socket ...\n", serverIdx);
        }

        struct sockaddr_in remote_address;
        socklen_t remote_address_len;
        remote_address_len = sizeof(address);

        //printf("[serverAM %ld] Accepting for socket ...\n", serverIdx); // verbose
        server_recv_socket[serverIdx] = accept(my_socket, (struct sockaddr*) &remote_address, &remote_address_len);

        if (server_recv_socket[serverIdx] < 0) {
            flag = false;
        }
    }

    // continously listen to arrived message
    while (flag) {
        //status buffer0;
        char recv_message_buffer[buffer_size];
        bzero(recv_message_buffer, buffer_size);
        int res = recv(server_recv_socket[serverIdx], recv_message_buffer, buffer_size, 0); // serialize
        string recv_message_str = recv_message_buffer;
        status buffer0 = deserialize_string(recv_message_str);

        if (res > 0) {
            if (buffer0.mode == -1) {

            } else if (buffer0.mode == 0) {//mode 0; buffer with message
                // when receiving message from a failed node
                if ((abadon_node_mask & (1 << buffer0.sender_num)) != 0) continue;

                mtx.lock();
                total_sequence_num += 1; // 1.4.1 sequence number + 1;
                buffer0.seq_num = total_sequence_num;
                buffer0.proposer_id = vmIndex;
                pq.insert(buffer0);
                node_message_left[buffer0.sender_num]++;
                mtx.unlock();

                buffer0.mode = 1;
                string propose_message_str = serialize_status(buffer0);
                bzero(recv_message_buffer, buffer_size);
                strcpy(recv_message_buffer, propose_message_str.c_str());
                //printf("I have proposed seq: %s\n", propose_message_str.c_str()); // verbose
                res = send(client_send_socket[serverIdx], recv_message_buffer, buffer_size, 0);
                if(res < 0) {
                    // modify abandon_mask and alive_mask
                    int bitmask = 1 << serverIdx;
                    abadon_node_mask |= bitmask;
                    bitmask = ~bitmask;
                    alive_node_mask &= bitmask;
                    flag = false;

                    printf("[serverAM %ld] Fail to response!",serverIdx);
                }

            } else if(buffer0.mode == 1) {//mode==1 the sender recevies feedback from vms
                int m_index = buffer0.message_id; //the message id; which is also the index of f and p;
                int s_seq = buffer0.seq_num; //suggested sequence recevied 

                mtx.lock();
                proposed_sequence_bitmask.find(m_index)->second |= (1 << serverIdx);

                //this is used for the update of the sender's final sequence numbers
                if(sent_seq_proposer[m_index].first < s_seq) {
                    sent_seq_proposer[m_index].first = s_seq;
                    sent_seq_proposer[m_index].second = serverIdx; 
                } else if((sent_seq_proposer[m_index].first == s_seq) && (sent_seq_proposer[m_index].second < serverIdx)) {
                    sent_seq_proposer[m_index].second = serverIdx;
                }

                //this is used for update the message in the priority_queue
                if((proposed_sequence_bitmask.find(m_index)->second & alive_node_mask) == alive_node_mask) {
                    for(const auto& i:pq) {
                        if((i.message_id == m_index) && (i.sender_num == vmIndex)) {
                            status New;
                            New = i;
                            New.seq_num = sent_seq_proposer[m_index].first;
                            if (New.seq_num > total_sequence_num) total_sequence_num = New.seq_num;
                            New.proposer_id = sent_seq_proposer[m_index].second;
                            New.deliverable = true;
                            //New.message_str = i.message_str;
                            pq.erase(i);
                            pq.insert(New);
                            break;
                        }
                    }
                    SendMessage("",2,m_index,sent_seq_proposer[m_index]);

                    proposed_sequence_bitmask.erase(m_index); // bitmask
                    sent_seq_proposer.erase(m_index);
                }
                mtx.unlock();

            } else if(buffer0.mode == 2) {
                int m_index = buffer0.message_id; //the message id; which is also the index of f and p;
                int s_seq = buffer0.seq_num; //suggested sequence recevied 
                
                
                mtx.lock();
                if (total_sequence_num < s_seq) total_sequence_num = s_seq; // 1.4.1 update seq_num to max agreed seq num
                for(const auto& i:pq) {
                    if(i.message_id == m_index && i.sender_num == serverIdx)//match with the one who send the message
                    {
                        status New;
                        New = i;
                        New.seq_num = s_seq;
                        New.proposer_id = buffer0.proposer_id;
                        New.deliverable = true;
                        //New.message_str = i.message_str;
                        pq.erase(i);
                        pq.insert(New);
                        break;
                    }
                } 
                mtx.unlock();

            } else if (buffer0.mode == 3) { // someone announce the offline_node

            } else if (buffer0.mode == 4) {
                client_heartbeat_received[serverIdx] = true;
            }                
        } else {//res==-1   
            printf("[serverAM %ld] server shutdown...\n", serverIdx);

            // modify abandon_mask and alive_mask
            int bitmask = 1 << serverIdx;
            abadon_node_mask |= bitmask;
            bitmask = ~bitmask;
            alive_node_mask &= bitmask;
            flag = false;

            // clear socket
            shutdown(client_send_socket[serverIdx], 2);
            close(client_send_socket[serverIdx]);
            client_send_socket[serverIdx] = -1;

            mtx.lock();
            if (node_message_left[serverIdx] == 0) {
                printf("Let's say good bye to node %d\n", serverIdx);
                node_message_left[serverIdx] = -1;
            }
            mtx.unlock();
        }     
    } // end of while(flag)

    // close this server and clear up
    aliveServer--;
    shutdown(my_socket, 2);
    close(server_recv_socket[serverIdx]);
    close(my_socket);
    //printf("[serverAM %ld] Exiting server code...\n", serverIdx); // verbose
    pthread_exit(NULL);
}

void* hb_do_client(void*) {
    //printf("[client] hb Start to broadcast connect...\n"); // verbose
    for(int i = 0; i < vmNum; i++) {
        if (i == vmIndex) continue;
        
        hb_client_send_socket[i]=socket(PF_INET, SOCK_STREAM, 0);
        if(hb_client_send_socket[i] < 0) printf("[hb client] Socket %d creation failed in client...\n", i);
        
        struct sockaddr_in address;
        address.sin_family = AF_INET;
        
        // set server address
        if (localTest == 2) { // vm test
            struct hostent *server;
            string vm=patch::to_string(i + 1);
            string server_name;
            if (i == 9) server_name = "sp17-cs425-g17-10.cs.illinois.edu";
            else server_name="sp17-cs425-g17-0" + vm + ".cs.illinois.edu";
            
            char *server_name_char = (char *)alloca(server_name.size() + 1);
            memcpy(server_name_char, server_name.c_str(), server_name.size() + 1);
            server = gethostbyname(server_name_char);
            if (server == NULL) {
                fprintf(stderr,"ERROR, no such host\n");
                exit(0);
            }
            bcopy((char *)server->h_addr,
                  (char *)&address.sin_addr.s_addr,
                  server->h_length);
        } else { // local test
            address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        }
        
        address.sin_port = htons(portBase + 500 + serverRange * i + vmIndex);
        int res = connect( hb_client_send_socket[i], (struct sockaddr*) &address, sizeof(struct sockaddr));
        if(res < 0) {
            hb_client_send_socket[i] = -1;
            printf("[hb client] Trouble connecting %d ...\n", i);
        } else {
            //alive_node_mask |= (1 << i);
            // 1.4.4
            pthread_t client_send_heartbeat_t; 
            long hb_send_idx = i;
            int res = pthread_create(&client_send_heartbeat_t, NULL, send_heartbeat_thread, (void *)hb_send_idx);
            
            if (res < 0){
                std::cout << "[server] Error:unable to create thread," << res << std::endl;
                exit(-1);
            }
        }
    }

    while (self_alive) ;
    
    // shutdown and close all client sockets
    for(int i = 0; i < vmNum; i++) {
        if (i == vmIndex || hb_client_send_socket[i] < 0) continue;

        shutdown(hb_client_send_socket[i], 2);
        close(hb_client_send_socket[i]);
        hb_client_send_socket[i] = -1;
    }
    
    pthread_exit(NULL);
}

void* send_heartbeat_thread(void* param) {
    long serverIdx = (long)param;
    unsigned int sleep_microseconds = max_delay_ms * 1000; // sleep for 100ms
    char message_buffer[buffer_size];
    bzero(message_buffer, buffer_size);
    string seri_message = "4|";
    strcpy(message_buffer, seri_message.c_str());

    while (self_alive && hb_client_send_socket[serverIdx] > 0) { // only send heartbeat when self alive
        // send heartbeat
        int res = send(hb_client_send_socket[serverIdx], message_buffer, buffer_size, 0);
        if (res < 0) {
            printf("605 error %ld\n", serverIdx);
            hb_client_send_socket[serverIdx] = -1;
        }

        usleep(sleep_microseconds);
    }

    //printf("[hb] See you, %ld!\n", serverIdx); // verbose
    pthread_exit(NULL);
}

void* do_client(void*) {
    //printf("[client] Start to broadcast connect...\n"); // verbose
    for(int i = 0; i < vmNum; i++) {
        if (i == vmIndex) continue;
        
        client_send_socket[i]=socket(PF_INET, SOCK_STREAM, 0);
        if(client_send_socket[i] < 0) printf("[client] Socket %d creation failed in client...\n", i);
        
        struct sockaddr_in address;
        address.sin_family = AF_INET;
        
        // set server address
        if (localTest == 2) { // vm test
            struct hostent *server;
            string vm=patch::to_string(i + 1);
            string server_name;
            if (i == 9) server_name = "sp17-cs425-g17-10.cs.illinois.edu";
            else server_name="sp17-cs425-g17-0" + vm + ".cs.illinois.edu";
            
            char *server_name_char = (char *)alloca(server_name.size() + 1);
            memcpy(server_name_char, server_name.c_str(), server_name.size() + 1);
            server = gethostbyname(server_name_char);
            if (server == NULL) {
                fprintf(stderr,"ERROR, no such host\n");
                exit(0);
            }
            bcopy((char *)server->h_addr,
                  (char *)&address.sin_addr.s_addr,
                  server->h_length);
        } else { // local test
            address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        }
        
        address.sin_port = htons(portBase + serverRange * i + vmIndex);
        int res = connect( client_send_socket[i], (struct sockaddr*) &address, sizeof(struct sockaddr));
        if(res < 0) {
            client_send_socket[i] = -1;
            printf("[client] Trouble connecting %d ...\n", i);
        } else {
            alive_node_mask |= (1 << i);
        }
    }

    // continuously accept input from user
    bool flag = true;
    while (flag) {
        printf("[client] Input message: ");
        string send_text;
        getline(cin,send_text);
        //if (send_text.length() == 0) flag = false;

        if(send_text[0]=='!') {
            flag = false;
            send_text = '!';
            // shutdown all alive server of itself
            for (int i = 0; i < vmNum; i++) {
                if (i != vmIndex) {
                    shutdown(server_recv_socket[i], 2);//exiting the server first
                }
            }
        } else if (send_text[0]=='@') {
            mtx.lock();
            for(const auto& i:pq) {
                printf("status from vm%d, message: %s\n", i.sender_num, i.message_str.c_str());
                //if (i.sender_num == vmIndex) printf("bitmask: %d\n", proposed_sequence_bitmask.find(i.message_id)->second);
                if (i.deliverable) printf("it can be delivered\n");
            }
            mtx.unlock();

        }  else if (send_text[0]=='v') {
            printf("version: %s\n", version_code.c_str());
        } else {
            total_message_id += 1; //every time a new message is create, message id + 1
            pair<int,int> temp(0,0);

            //pairs initialization
            mtx.lock();
            total_sequence_num += 1; // 1.4.1
            sent_seq_proposer.insert(std::make_pair(total_message_id,make_pair(total_sequence_num,vmIndex)));
            proposed_sequence_bitmask.insert(std::make_pair(total_message_id,0));

            SendMessage(send_text,0,total_message_id,temp); //send_text, mode, message id, pair(not used here)
            mtx.unlock();
        }
    }
    
    // shutdown and close all client sockets
    for(int i = 0; i < vmNum; i++) {
        if (i == vmIndex || client_send_socket[i] < 0) continue;

        shutdown(client_send_socket[i], 2);
        close(client_send_socket[i]);
        client_send_socket[i] = -1;
    }
    
    self_alive = false;
    pthread_exit(NULL);
}

void* SendMessage(string send_text,int input_mode,int input_message_id,pair<int,int> pa)
{
    status buffer;
    buffer.message_id = input_message_id; // when mode == 3, this is the offline node_idx
    buffer.mode = input_mode;
    buffer.sender_num = vmIndex;
    buffer.message_str = send_text;

    if(input_mode == 0) // send message 
    {   
        buffer.seq_num = total_sequence_num;
        buffer.proposer_id = vmIndex;
        pq.insert(buffer);
        //printf("[client] Message got! Start to send\n"); // verbose
    }
    else if(input_mode == 2)
    {
        buffer.seq_num = pa.first;
        buffer.proposer_id = pa.second;
    } else if (input_mode == 4 || input_mode == 3) { // heartbeat
        // do nothing...
    }

    int client_sent_cnt = 0;
    string seri_message = serialize_status(buffer);
    //if (input_mode != 4) printf("serialized string: %s\n", seri_message.c_str()); // verbose
    char message_buffer[buffer_size];
    bzero(message_buffer, buffer_size);
    strcpy(message_buffer, seri_message.c_str());
    if (input_mode == 4) {
        for(int i = 0; i < vmNum; i++) {
            if(i != vmIndex && hb_client_send_socket[i] > 0 && (alive_node_mask & (1 << i)) != 0) { // bitmask 
                //int res=send(client_send_socket[i], &buffer, sizeof(buffer), 0);
                int res = send(hb_client_send_socket[i], message_buffer, buffer_size, 0);
                if( res < 0) cout << "ERROR CPT1" << endl;
            }
        
        }
    } else { // of if (input_mode == 4)
        for(int i = 0; i < vmNum; i++) {
            if(i != vmIndex && client_send_socket[i] > 0 && (alive_node_mask & (1 << i)) != 0) { // bitmask 
                client_sent_cnt++;
                //int res=send(client_send_socket[i], &buffer, sizeof(buffer), 0);
                int res = send(client_send_socket[i], message_buffer, buffer_size, 0);
                if( res < 0) cout << "ERROR CPT1" << endl;
            }
        }
        //if (input_mode == 2) printf("734 I've told %d nodes\n", client_sent_cnt); // verbose
    }
}

string serialize_status(status from_status) {
    // mode 0: mode|message_id|sender_num|message
    // mode 1: mode|message_id|seq_num|proposer_id
    // mode 2: mode|message_id|seq_num|proposer_id
    // mode 3: mode|offline_node_idx
    // mode 4: mode
    string to_string = "";
    if (from_status.mode == 0) {
        // mode
        to_string += patch::to_string(from_status.mode);
        to_string +="|";

        // message_id
        to_string += patch::to_string(from_status.message_id);
        to_string +="|";

        // sender_num
        to_string += patch::to_string(from_status.sender_num);
        to_string +="|";

        // message_str
        to_string += from_status.message_str;
    } else if (from_status.mode == 1 || from_status.mode == 2) {
        // mode
        to_string += patch::to_string(from_status.mode);
        to_string +="|";

        // message_id
        to_string += patch::to_string(from_status.message_id);
        to_string +="|";

        // seq_num
        to_string += patch::to_string(from_status.seq_num);
        to_string +="|";

        // proposer_id
        to_string += patch::to_string(from_status.proposer_id);
    } else if (from_status.mode == 3) {
        // mode
        to_string += patch::to_string(from_status.mode);
        to_string +="|";
        to_string += patch::to_string(from_status.message_id);
    } else if (from_status.mode == 4) {
        // mode
        to_string += patch::to_string(from_status.mode);
        to_string +="|";
    }
    
    return to_string;
}

status deserialize_string(string from_string) {
    status to_status;
    if (from_string.length() < 1) {
        //printf("what did you send? %s\n", from_string.c_str()); // verbose
        to_status.mode = 4; // for case of loss random
        return to_status;
    }

    // mode
    int pos_mode = from_string.find("|");
    if (pos_mode == std::string::npos || pos_mode == 0 || pos_mode > 2) {
        to_status.mode = -1;
        //printf("what did you send? %s\n", from_string.c_str()); // verbose
        return to_status;
    }

    int mode = stoi(from_string.substr(0, pos_mode));
    if (mode == 0) {
        //printf("0 from_string: %s\n", from_string.c_str()); // verbose
        to_status.mode = mode;

        // message_id
        int pos_message_id = from_string.find("|", pos_mode + 1);
        to_status.message_id = stoi(from_string.substr(pos_mode + 1, pos_message_id - pos_mode - 1));

        // sender_num
        int pos_sender_num = from_string.find("|", pos_message_id + 1);
        to_status.sender_num = stoi(from_string.substr(pos_message_id + 1, pos_sender_num - pos_message_id - 1));

        // message_str
        to_status.message_str = from_string.substr(pos_sender_num + 1);
    } else if (mode == 1 || mode == 2) {
        to_status.mode = mode;

        // message_id
        int pos_message_id = from_string.find("|", pos_mode + 1);
        to_status.message_id = stoi(from_string.substr(pos_mode + 1, pos_message_id - pos_mode - 1));

        // seq_num
        int pos_seq_num = from_string.find("|", pos_message_id + 1);
        to_status.seq_num = stoi(from_string.substr(pos_message_id + 1, pos_seq_num - pos_message_id - 1));

        // proposer_id
        to_status.proposer_id = stoi(from_string.substr(pos_seq_num + 1));
    } else if (mode == 4) {
        to_status.mode = mode;
        if (from_string.length() > 2) printf("str after 4: %s\n", from_string.substr(2).c_str());
    }
    return to_status;
}

void* recv_heartbeat(void* param) {
    long serverIdx = (long)param;
    //printf("listening to hb %d\n", serverIdx); // verbose
    client_heartbeat_received[serverIdx] = true;
    unsigned int sleep_microseconds = max_delay_ms * lazy * 1000; // sleep for lazy * 100ms

    while (self_alive && client_heartbeat_received[serverIdx] == true) {
        client_heartbeat_received[serverIdx] = false;

        usleep(sleep_microseconds);
    }

    // modify abandon_mask and alive_mask
    int bitmask = 1 << serverIdx;
    abadon_node_mask |= bitmask;
    bitmask = ~bitmask;
    alive_node_mask &= bitmask;

    if (hb_client_send_socket[serverIdx] > 0) {
        shutdown(hb_client_send_socket[serverIdx], 2);
        close(hb_client_send_socket[serverIdx]);
        hb_client_send_socket[serverIdx] = -1;
    }
    //printf("[HeartBeat %ld] RIP...\n", serverIdx); // verbose

    mtx.lock();
    if (node_message_left[serverIdx] == 0) {
        printf("Let's say good bye to node %d\n", serverIdx);
        node_message_left[serverIdx] = -1;
    }
    mtx.unlock();

    pthread_exit(NULL);
}

void* send_heartbeat(void* param) {
    unsigned int sleep_microseconds = max_delay_ms * 1000; // sleep for 100ms

    while (self_alive) { // only send heartbeat when self alive
        pair<int,int> temp(0,0);
        mtx.lock();
        SendMessage("",4,0,temp);
        mtx.unlock();

        usleep(sleep_microseconds);
    }
    //printf("See you next time!\n"); // verbose
    pthread_exit(NULL);
}

void* message_delivery(void* param) {
    unsigned int sleep_microseconds = 5 * max_delay_ms * 1000; // sleep for 500ms
    //printf("Welcome to message center!\n"); // verbose

    while (self_alive) { // only send heartbeat when self alive
        mtx.lock();
        if(!pq.empty()) {
            bool pq_iterate_flag = true;

            while (pq_iterate_flag) {
                if (pq.empty()) pq_iterate_flag = false;
                else if (pq.begin()->deliverable) {
                    printf("Message from %d: %s\n", pq.begin()->sender_num, pq.begin()->message_str.c_str());
                    
                    int sender_idx = pq.begin()->sender_num;
                    if (sender_idx != vmIndex) { // when sender is other node
                        node_message_left[sender_idx]--;
                        if ((abadon_node_mask & (1 << sender_idx)) != 0 && node_message_left[sender_idx] == 0) {
                            printf("Let's say good bye to node %d\n", sender_idx);
                            node_message_left[sender_idx] = -1;
                        }
                    }
                    
                    pq.erase(pq.begin());
                } else if ((abadon_node_mask & (1 << pq.begin()->sender_num)) != 0) {
                    int sender_idx = pq.begin()->sender_num;
                    node_message_left[sender_idx]--;
                    if (node_message_left[sender_idx] == 0) {
                        printf("Let's say good bye to node %d\n", sender_idx);
                        node_message_left[sender_idx] = -1;
                    }

                    pq.erase(pq.begin());
                } else if (pq.begin()->sender_num == vmIndex) {
                    int m_index = pq.begin()->message_id;
                    if ((proposed_sequence_bitmask.find(m_index)->second & alive_node_mask) == alive_node_mask) {
                        for(const auto& i:pq) {
                            if((i.message_id == m_index) && (i.sender_num == vmIndex)) {
                                status New;
                                New = i;
                                New.seq_num = sent_seq_proposer[m_index].first;
                                if (New.seq_num > total_sequence_num) total_sequence_num = New.seq_num;
                                New.proposer_id = sent_seq_proposer[m_index].second;
                                New.deliverable = true;
                                
                                pq.erase(i);
                                pq.insert(New);
                                break;
                            }
                        }
                        SendMessage("",2,m_index,sent_seq_proposer[m_index]);

                        proposed_sequence_bitmask.erase(m_index); // bitmask
                        sent_seq_proposer.erase(m_index);
                    } else pq_iterate_flag = false;
                } else pq_iterate_flag = false;
            }
        } 
        mtx.unlock();

        usleep(sleep_microseconds);
    } 

    pthread_exit(NULL);
}