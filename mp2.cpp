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

vector<string> split(string str, char delimiter) {
    vector<string> internal;

    // segment 1
    int pos_seg1 = str.find(" ");
    if (pos_seg1 == std::string::npos || pos_seg1 < 0) {
        internal.push_back(str);
        return internal;
    }

    internal.push_back(str.substr(0, pos_seg1));
    
    // segment 2
    int pos_seg2 = str.find(" ", pos_seg1 + 1);
    if (pos_seg2 == std::string::npos || pos_seg2 < 0) {
        internal.push_back(str.substr(pos_seg1 + 1));
        return internal;
    }

    internal.push_back(str.substr(pos_seg1 + 1, pos_seg2 - pos_seg1 - 1));

    // segment 3
    internal.push_back(str.substr(pos_seg2 + 1));
  
    return internal;
}

mutex mtx_replica_confirm; // mutex for replica confirm
mutex mtx_set_buffer; // mutex for buffers
mutex mtx_recovery; // mutex for recovery(node fail)
mutex mtx_join; // mutext for join(node join)

string version_code = "1.4.1(testing): modified warm join";
int sigErrorPrint; // count number of sig error printed
int vmNum = 10; // number of vms
int vmIndex; // index of this vm (start from 0)
int serverRange = 50;
int portBase = 4000; // can be modified by user
int buffer_size =512; // socket message buffer size
int localTest;
int server_recv_socket[10]; // server sockets
int client_send_socket[10]; // client sockets
int trustList = 0; // (bit mask of) nodes in majority group
int new_trustList = 0; // trust list after node change
bool replyArrived = false; // flag whether remote operation reply has arrived
string replyBuffer; // buffer to store reply
int replicaConfirm; // number of confirm from replica
int mssgLengths[10]; // message length array
int recoveryNodes = 0; // number of nodes under recovery
int failedNode1 = -1; // failed node 1 idx
int failedNode2 = -1; // failed node 2 idx
bool fastMerged = false; // already fast merged?
int localRecovery1Cnt = 0; // number of local recovery 1 confirmed
int localRecovery2Cnt = 0; // number of local recovery 2 confirmed
bool hasData = false; // whether node has data
bool vm_up[10]; // 1 whether the vm is up (passively detect)
bool clusterHasData = false; // whether the cluster has data
bool waitingWelcome = false; // whether the waiting_for_welcome thread has been launched
int connected_trustList = 0; // trust list of connected nodes
bool joinedIntoCluster = false; // whether this node successfully joined into cluster
int fastSetReceived = 0; // fast set received when warm join cluster

unordered_map<string, string> storageMap;
unordered_map<int, unordered_set<string> > dataListMap;

queue<string> keyQueue;
queue<string> valueQueue;
queue<int> requesterQueue;

void* acceptConnection(void*);
void* buildConnection(void*);
void* commandInput(void*);
void deleteListAndData(int toDelete);
int findSuccessorNodeRecovery(int nodeIdx);
int findPredecessorNodeRecovery(int nodeIdx);
int findMasterNode(string key, int trustList_here);
int findReplicaNode1(int masterNode);
int findReplicaNode2(int masterNode);
void fastSet(int receiver, int masterNode, int confirmIdx);
void fastSetData(int masterNode, int nums, string dataStr, int pos_start);
string getLocal(string key);
void* initializeServer(void*);
void* initializeClient(void*);
string listLocal();
void mergeAndClearList(int mergeTo, int mergeFrom);
string ownersLocal(int masterNode);
string preProcessCommand(string commandStr);
int readAndExcute(string recv_message_str, int sender);
bool replicateData(string key, string value);
void* recovery_thread(void*);
void* setLocal_thread(void*);
string setLocal(string key, string value, int masterNode);
void* SendMessage(int receiver, int type, string key, string value);
void splitList(int splitFrom, int splitTo);
void storeDataAndUpdateList(string key, string value, int masterNode);
void updateDataList(int masterNode, string key);
void* waitWelcome_thread(void*);
void welcomeNewNode(int newNode);

// message type:
// type: 0 exit; 1 set; 2 get; 3 set reply; 4 get reply; 5 replicate set; 6 set done in replicas; 
// 7 length message; 8: fast set 9: local recovery complete 10: recovery complete 11: setData
// 12: welcome message from node when node join 13: new node joined
// 100 ignore; 101 unknown type

/* Catch Signal Handler function */
void signal_callback_handler(int signum) {
    if (sigErrorPrint < 3) {
        printf("sigpipe error!\n");
        sigErrorPrint++;
    }
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        fprintf(stderr,"ERROR, not enough params provided\n");
        exit(1);
    }
    vmIndex = atoi(argv[1]);
    localTest = atoi(argv[2]); // 1 for local, 2 for vm

    // register SIGPIPE
    signal(SIGPIPE, signal_callback_handler);

    // initialize data
    trustList |= (1 << vmIndex);

    for (int i = 0; i < 10; i++) {
        std::unordered_set<string> myset;
        dataListMap.insert(std::make_pair(i,myset));
    }

    for (int i = 0; i < 10; i++) {
        vm_up[i] = true;
    }
    
    // create threads
    pthread_t serverThread, clientThread, commandThread, setLocalThread;
    int rc = pthread_create(&serverThread, NULL, initializeServer, NULL);
    if (rc){
        std::cout << "Error:unable to create thread," << rc << std::endl;
        exit(-1);
    }
    sched_yield();
    sleep(1);

    rc = pthread_create(&clientThread, NULL, initializeClient, NULL);
    if (rc){
        std::cout << "Error:unable to create thread," << rc << std::endl;
        exit(-1);
    }
    
    rc = pthread_create(&commandThread, NULL, commandInput, NULL);
    if (rc){
        std::cout << "Error:unable to create thread," << rc << std::endl;
        exit(-1);
    }

    // create thread to set data in replicas
    rc = pthread_create(&setLocalThread, NULL, setLocal_thread, NULL);
    if (rc){
        std::cout << "[read&execute] Error:unable to create thread," << rc << std::endl;
        exit(-1);
    }
    
    pthread_exit(NULL);
    return 0;
}

void* initializeServer(void*) {
    pthread_t messageThreads[vmNum];
    long idxes[vmNum];
        
    // when it is its turn to accept
    for (int j = 0; j < vmNum; j++) {
        if (j == vmIndex) continue;

        idxes[j] = j;
        int rc = pthread_create(&messageThreads[j], NULL, acceptConnection, (void *)idxes[j]);
        if (rc){
            std::cout << "[server] Error:unable to create thread," << rc << std::endl;
            exit(-1);
        }
    }
       
    printf("[server] All sockets ready ...\n");
    
    pthread_exit(NULL);
}

void* acceptConnection(void* param) {
    long serverIdx = (long)param;
    int my_socket = socket(PF_INET, SOCK_STREAM, 0);
    if(my_socket < 0) printf("[serverAM %ld] Socket creation failed ...\n", serverIdx);
    struct sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port = htons(portBase + serverRange * vmIndex + serverIdx);
    bool flag = true;

    int yes = 1;
    if (setsockopt(my_socket, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) == -1) {
        printf("[serverAM %ld] Trouble set sockopt ...\n", serverIdx);
        perror("setsockopt");
        exit(1);
    }
    
    int res = bind(my_socket, (struct sockaddr*) &address, sizeof(struct sockaddr_in));
    if(res< 0) {
        flag = false;
        printf("[serverAM %ld] Trouble binding socket ...\n", serverIdx);
    }

    struct sockaddr_in remote_address;
    socklen_t remote_address_len;
    remote_address_len = sizeof(address);
    
    while (flag) {
        // reconnect
        res = listen(my_socket, 5);
        if(res < 0) {
            flag = false;
            printf("[serverAM %ld] Couldn't listen to socket ...\n", serverIdx);
            break;
        }

        server_recv_socket[serverIdx] = accept(my_socket, (struct sockaddr*) &remote_address, &remote_address_len);
        printf("[serverAM %ld] Server connected ...\n", serverIdx);

        vm_up[serverIdx] = true;

        // update trust list
        // trustList |= (1 << serverIdx); // join
        mtx_join.lock();
        
        if (joinedIntoCluster) {
            if (!hasData) trustList |= (1 << serverIdx);
            else {
                // need to wait for data copy
                recoveryNodes++;
                printf("[serverAM %ld] I'm waiting for this node to join\n", serverIdx);
            }
        } else {
            connected_trustList |= (1 << serverIdx);
            // this is the new node
            if (!waitingWelcome) {
                waitingWelcome = true;
                pthread_t waitWelcomeThread;
                int rc = pthread_create(&waitWelcomeThread, NULL, waitWelcome_thread, NULL);
                if (rc){
                    std::cout << "[server] Error:unable to create thread," << rc << std::endl;
                    exit(-1);
                }
            }
        }

        mtx_join.unlock();

        // accept message
        char buffer[buffer_size];
        while (flag) {
            bzero(buffer,buffer_size);
            res = recv(server_recv_socket[serverIdx], buffer, buffer_size, 0);

            if (res != 0) {
                string recv_message_str = buffer;

                int type = readAndExcute(recv_message_str, (int)serverIdx);

                // lengthy message
                if (type == 7) {
                    int mssg_length = mssgLengths[serverIdx];
                    string lengthyMssg = "";

                    for (int i = 0; i < mssg_length; i++) {
                        bzero(buffer,buffer_size);
                        res = recv(server_recv_socket[serverIdx], buffer, buffer_size, 0);

                        // in case sender fail at middle
                        if (res <= 0) {
                            type = 0;
                            break;
                        }

                        string part_message_str = buffer;
                        lengthyMssg += part_message_str;
                    }

                    if (type == 7) {
                        readAndExcute(lengthyMssg, (int)serverIdx);
                    }
                }

                if (type <= 0) flag = false;
            } else {
                flag = false;
            }
        }

        shutdown(server_recv_socket[serverIdx], 2);
        shutdown(client_send_socket[serverIdx], 2);
        close(client_send_socket[serverIdx]);
        close(server_recv_socket[serverIdx]);

        client_send_socket[serverIdx] = -1;

        printf("[serverAM %ld] server shutdown...\n", serverIdx);

        // update trust list
        mtx_recovery.lock();
        recoveryNodes++;

        int bitMask = 1 << serverIdx;
        bitMask = ~bitMask;

        if (failedNode1 == -1) {
            // first failed node
            failedNode1 = serverIdx;
            new_trustList = trustList;
            new_trustList &= bitMask;

            // create thread for recovery
            pthread_t recoveryThread;
            int rc = pthread_create(&recoveryThread, NULL, recovery_thread, NULL);
            if (rc){
                std::cout << "Error:unable to create thread, " << rc << std::endl;
                exit(-1);
            }
        } else {
            // second failed node
            failedNode2 = serverIdx;
            new_trustList &= bitMask;
        }
        mtx_recovery.unlock();

        // need to stop here to wait for recovery
        while (recoveryNodes > 0) {
            usleep(1000);
        }

        flag = true;       
    }

    shutdown(my_socket, 2);
    close(my_socket);
    printf("[serverAM %ld] Exiting server code...\n", serverIdx);
    pthread_exit(NULL);
}

void* waitWelcome_thread(void*) {
    usleep(50 * 1000);

    // cold join
    if (!clusterHasData) {
        printf("Cold joined!\n");
    } else { // warm join
        hasData = true;

        // wait for fastset from other node
        while (fastSetReceived < 3) {
            usleep(1000);
        }

        fastSetReceived = 0;

        // broadcast joined
        for (int i = 0; i < 10; i++) {
            if (i != vmIndex && (connected_trustList & (1 << i)) != 0) 
                SendMessage(i, 13, "", "");
        }

        printf("Warm joined!\n");
    }

    joinedIntoCluster = true;

    trustList |= connected_trustList;
    connected_trustList = 0;

    pthread_exit(NULL);
}

void* recovery_thread(void*) {
    // wait some time for detection of second failed node
    sleep(1);

    localRecovery1Cnt = 0;
    localRecovery2Cnt = 0;

    int tmpSmallerFailedNode = failedNode1 < failedNode2 ? failedNode1 : failedNode2;
    int tmpLargerFailedNode = failedNode1 > failedNode2 ? failedNode1 : failedNode2;

    int responsibleNodeLarger = findSuccessorNodeRecovery(tmpLargerFailedNode);
    int predecessorLarger = findPredecessorNodeRecovery(tmpLargerFailedNode);

    int responsibleNodeSmaller = findSuccessorNodeRecovery(tmpSmallerFailedNode);
    int predecessorSmaller = findPredecessorNodeRecovery(tmpSmallerFailedNode);

    if (tmpSmallerFailedNode != -1) {      
        // pre large small respon
        if (tmpSmallerFailedNode == findReplicaNode2(tmpLargerFailedNode)) {
            // this is a tricky use of findRN2, which finds the successor of certain node
            // that means they are continual
            if (vmIndex == findReplicaNode1(tmpLargerFailedNode)) {
                // fast merge to next node
                fastSet(responsibleNodeSmaller, tmpLargerFailedNode, 0);
            } else if (vmIndex == responsibleNodeSmaller) {
                while (!fastMerged) {
                    // wait
                }

                fastMerged = false;
            }
        } else if (tmpLargerFailedNode == findReplicaNode2(tmpSmallerFailedNode)) {
            // pre small large respon
            if (vmIndex == predecessorSmaller) {
                // fast merge to next node
                fastSet(responsibleNodeSmaller, tmpSmallerFailedNode, 0);
            } else if (vmIndex == responsibleNodeLarger) {
                while (!fastMerged) {
                    // wait
                }

                fastMerged = false;
            }
        }
    }
    
    // merge two list (larger)
    mergeAndClearList(responsibleNodeLarger, tmpLargerFailedNode);
    if (tmpSmallerFailedNode != -1) {
        // merge two list (smaller)
        mergeAndClearList(responsibleNodeSmaller, tmpSmallerFailedNode);
    }

    if (vmIndex == predecessorLarger) {
        fastSet(responsibleNodeLarger, vmIndex, 0);
    } else if (vmIndex == responsibleNodeLarger) {
        fastSet(findSuccessorNodeRecovery(vmIndex), vmIndex, 1);
        fastSet(findPredecessorNodeRecovery(vmIndex), vmIndex, 1);

        while (localRecovery1Cnt < 2) {
            // wait
            usleep(1000);
        }

        // broadcast recovery complete
        for (int i = 0; i < 10; i++) {
            if (i != vmIndex && (new_trustList & (1 << i)) != 0) 
                SendMessage(i, 10, "", "");
        }

        recoveryNodes--;

        if (responsibleNodeSmaller == responsibleNodeLarger) {
            // broadcast recovery complete
            for (int i = 0; i < 10; i++) {
                if (i != vmIndex && (new_trustList & (1 << i)) != 0) 
                    SendMessage(i, 10, "", "");
            }

            recoveryNodes--;
        }
    }

    // recover smaller node
    if (tmpSmallerFailedNode != -1 && responsibleNodeSmaller != responsibleNodeLarger) {
        // recover smaller node
        if (vmIndex == predecessorSmaller) {
            fastSet(responsibleNodeSmaller, vmIndex, 0);
        } else {
            if (vmIndex == responsibleNodeSmaller) {
                fastSet(findSuccessorNodeRecovery(vmIndex), vmIndex, 2);
                fastSet(findPredecessorNodeRecovery(vmIndex), vmIndex, 2);

                while (localRecovery2Cnt < 2) {
                    // wait
                    usleep(1000);
                }

                // broadcast recovery complete
                for (int i = 0; i < 10; i++) {
                    if (i != vmIndex && (new_trustList & (1 << i)) != 0) 
                        SendMessage(i, 10, "", "");
                }

                recoveryNodes--;
            }
        }
    }

    // need to stop here to wait for recovery
    while (recoveryNodes > 0) {
        usleep(1000);
    }

    trustList = new_trustList;
    new_trustList = 0;

    failedNode1 = -1;
    failedNode2 = -1;

    localRecovery1Cnt = 0;
    localRecovery2Cnt = 0;

    pthread_exit(NULL);
}

void mergeAndClearList(int mergeTo, int mergeFrom) {
    // merge two list
    unordered_set<string> failedNodeList = dataListMap.find(mergeFrom)->second;
    unordered_set<string> selfNodeList = dataListMap.find(mergeTo)->second;

    for (string key : failedNodeList) selfNodeList.insert(key);

    dataListMap.find(mergeTo)->second = selfNodeList;

    unordered_set<string> emptyList;
    dataListMap.find(mergeFrom)->second = emptyList;
}

void fastSet(int receiver, int masterNode, int confirmIdx) {
    stringstream sendStrSS;

    sendStrSS << masterNode << ' ' << confirmIdx << ' ';

    unordered_set<string> keys = dataListMap.find(masterNode)->second;

    sendStrSS << keys.size();

    for (string key : keys) {
        sendStrSS << ' ' << key.length() << ' ' << key;

        string value = storageMap.find(key)->second;
        sendStrSS << ' ' << value.length() << ' ' << value;
    }

    SendMessage(receiver, 8, "", sendStrSS.str());
}

int findSuccessorNodeRecovery(int nodeIdx) {
    if (nodeIdx < 0) return -1;

    for (int i = 1; i < 10; i++) {
        int virtualIdx = nodeIdx + i;
        if (virtualIdx > 9) virtualIdx -= 10;

        if ((new_trustList & (1 << virtualIdx)) != 0) return virtualIdx;
    }
    return vmIndex;
}

int findPredecessorNodeRecovery(int nodeIdx) {
    if (nodeIdx < 0) return -1;

    for (int i = 1; i < 10; i++) {
        int virtualIdx = nodeIdx - i + 10;
        if (virtualIdx > 9) virtualIdx -= 10;

        if ((new_trustList & (1 << virtualIdx)) != 0) return virtualIdx;
    }
    return vmIndex;
}

int readAndExcute(string recv_message_str, int sender) {
    if (recv_message_str.length() < 1) {
        // what is this? E.T.?
        return -1;
    }

    int pos_mode = recv_message_str.find(" ");
    if (pos_mode == std::string::npos || pos_mode == 0) {
        return 100;
    }

    int mode = stoi(recv_message_str.substr(0, pos_mode));

    if (mode == 0) {
        return 0;
    } else if (mode == 1) {
        // key
        int pos_key = recv_message_str.find(" ", pos_mode + 1);
        int key_size = stoi(recv_message_str.substr(pos_mode + 1, pos_key - pos_mode - 1));
        string key = recv_message_str.substr(pos_key + 1, key_size);

        // value
        int pos_value = recv_message_str.find(" ", pos_key + 1 + key_size + 1);
        int value_size = stoi(recv_message_str.substr(pos_key + 1 + key_size + 1, pos_value - pos_key - 1));
        string value = recv_message_str.substr(pos_value + 1, value_size);

        // store key, value, and sender in buffer (producer-consumer)
        mtx_set_buffer.lock();
        keyQueue.push(key);
        valueQueue.push(value);
        requesterQueue.push(sender);
        mtx_set_buffer.unlock();

        return 1;
    } else if (mode == 2) {
        // key
        int pos_key = recv_message_str.find(" ", pos_mode + 1);
        int key_size = stoi(recv_message_str.substr(pos_mode + 1, pos_key - pos_mode - 1));
        string key = recv_message_str.substr(pos_key + 1, key_size);

        // get the value locally
        string get_result = getLocal(key);

        // send get result back
        SendMessage(sender, 4, "", get_result);

        return 2;
    } else if (mode == 3) {
        // set result
        int pos_set_result = recv_message_str.find(" ", pos_mode + 1);
        int result_size = stoi(recv_message_str.substr(pos_mode + 1, pos_set_result - pos_mode - 1));
        string set_result = recv_message_str.substr(pos_set_result + 1, result_size);

        // store set result into buffer
        replyArrived = true;
        replyBuffer = set_result;

        return 3;
    } else if (mode == 4) {
        // get result
        int pos_get_result = recv_message_str.find(" ", pos_mode + 1);
        int result_size = stoi(recv_message_str.substr(pos_mode + 1, pos_get_result - pos_mode - 1));
        string get_result = recv_message_str.substr(pos_get_result + 1, result_size);

        // store get result into buffer
        replyArrived = true;
        replyBuffer = get_result;

        return 4;
    } else if (mode == 5) {
        // key
        int pos_key = recv_message_str.find(" ", pos_mode + 1);
        int key_size = stoi(recv_message_str.substr(pos_mode + 1, pos_key - pos_mode - 1));
        string key = recv_message_str.substr(pos_key + 1, key_size);

        // value
        int pos_value = recv_message_str.find(" ", pos_key + 1 + key_size + 1);
        int value_size = stoi(recv_message_str.substr(pos_key + 1 + key_size + 1, pos_value - pos_key - 1));
        string value = recv_message_str.substr(pos_value + 1, value_size);

        // store the key-value pair into map locally
        storeDataAndUpdateList(key, value, sender);

        // send ack back
        SendMessage(sender, 6, "", "");

        return 5;
    } else if (mode == 6) {
        // required a lock
        mtx_replica_confirm.lock();
        replicaConfirm++;
        mtx_replica_confirm.unlock();

        return 6;
    } else if (mode == 7) {
        // get length
        int pos_mssg_length = recv_message_str.find(" ", pos_mode + 1);
        int mssg_length = stoi(recv_message_str.substr(pos_mode + 1, pos_mssg_length - pos_mode - 1));

        // store length into lengths array
        mssgLengths[sender] = mssg_length;

        return 7;
    } else if (mode == 8) {
        // master node
        int pos_master = recv_message_str.find(" ", pos_mode + 1);
        int masterNode = stoi(recv_message_str.substr(pos_mode + 1, pos_master - pos_mode - 1));

        // confirm idx
        int pos_confirmIdx = recv_message_str.find(" ", pos_master + 1);
        int confirmIdx = stoi(recv_message_str.substr(pos_master + 1, pos_confirmIdx - pos_master - 1));

        // nums of key-value
        int pos_nums = recv_message_str.find(" ", pos_confirmIdx + 1);
        int nums = stoi(recv_message_str.substr(pos_confirmIdx + 1, pos_nums - pos_confirmIdx - 1));

        if (confirmIdx == 3) {
            printf("received fast set message\n");

            // fast set in welcome stage
            fastSetData(masterNode, nums, recv_message_str, pos_nums);

            mtx_join.lock();
            fastSetReceived++;
            mtx_join.unlock();
        } else if (masterNode != sender) {
            // this is a fast merge
            fastSetData(vmIndex, nums, recv_message_str, pos_nums);
            fastMerged = true;
        } else {
            fastSetData(sender, nums, recv_message_str, pos_nums);
            // send local fastset complete
            if (confirmIdx == 1) {
                SendMessage(sender, 9, "1", "");
            } else {
                SendMessage(sender, 9, "2", "");
            }
        }
    } else if (mode == 9) {
        // confirm idx
        int pos_confirmIdx = recv_message_str.find(" ", pos_mode + 1);
        int confirmIdx = stoi(recv_message_str.substr(pos_mode + 1, pos_confirmIdx - pos_mode - 1));

        if (confirmIdx == 1) {
            mtx_recovery.lock();
            localRecovery1Cnt++;
            mtx_recovery.unlock();
        } else if (confirmIdx == 2) {
            mtx_recovery.lock();
            localRecovery2Cnt++;
            mtx_recovery.unlock();
        }

        return 9;
    } else if (mode == 10) {
        mtx_recovery.lock();
        recoveryNodes--;
        mtx_recovery.unlock();

        return 10;
    } else if (mode == 11) {
        hasData = true;

        return 11;
    } else if (mode == 12) {
        // hasDate
        int pos_hasData = recv_message_str.find(" ", pos_mode + 1);
        int hasDate_val = stoi(recv_message_str.substr(pos_mode + 1, pos_hasData - pos_mode - 1));

        if (hasDate_val == 1) {
            clusterHasData = true;
        }

        return 12;
    } else if (mode == 13) {
        trustList |= (1 << sender);
        recoveryNodes--;

        printf("received joined message, welcome!\n");

        return 13;
    }

    return 101;
}

void fastSetData(int masterNode, int nums, string dataStr, int pos_start) {
    int pos_key = 0;
    int pos_value = pos_start;
    int key_size = 0;
    int value_size = -1;
    for (int i = 0; i < nums; i++) {
        // key
        pos_key = dataStr.find(" ", pos_value + 1 + value_size + 1);
        key_size = stoi(dataStr.substr(pos_value + 1 + value_size + 1, pos_key - pos_value - 1));
        string key = dataStr.substr(pos_key + 1, key_size);

        // value
        pos_value = dataStr.find(" ", pos_key + 1 + key_size + 1);
        value_size = stoi(dataStr.substr(pos_key + 1 + key_size + 1, pos_value - pos_key - 1));
        string value = dataStr.substr(pos_value + 1, value_size);

        if (storageMap.find(key) == storageMap.end())
            storageMap.insert(std::make_pair(key,value));
        else
            storageMap.find(key)->second = value;

        dataListMap.find(masterNode)->second.insert(key);
    }
}

void* initializeClient(void*) {
    pthread_t messageThreads[vmNum];
    long idxes[vmNum];
        
    // when it is its turn to connect
    for (int j = 0; j < vmNum; j++) {
        if (j == vmIndex) continue;

        idxes[j] = j;
        int rc = pthread_create(&messageThreads[j], NULL, buildConnection, (void *)idxes[j]);
        if (rc){
            std::cout << "[server] Error:unable to create thread," << rc << std::endl;
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
        printf("[client] Socket %ld creation failed in client...\n", serverIdx);
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
            fprintf(stderr,"ERROR, no such host\n");
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
        if (!vm_up[serverIdx]) {
            usleep(1000);
            continue;
        }

        client_send_socket[serverIdx]=socket(PF_INET, SOCK_STREAM, 0);
        int res = connect(client_send_socket[serverIdx], (struct sockaddr*) &address, sizeof(struct sockaddr));
        if (res < 0) {
            
        } else {
            printf("[clientAM %ld] Server connected ...\n", serverIdx);

            // send welcome message
            if (hasData) {
                SendMessage(serverIdx, 12, "1", "");

                welcomeNewNode(serverIdx);
            } else SendMessage(serverIdx, 12, "0", "");

            while (res >= 0 && client_send_socket[serverIdx] > 0) {
                sleep(1);
            }

            printf("[clientAM %ld] Server disconnected ...\n", serverIdx);
        }

        vm_up[serverIdx] = false;
    }

    pthread_exit(NULL);
}

void welcomeNewNode(int newNode) {
    int predecessorNew = findReplicaNode1(newNode);
    int successorNew = findReplicaNode2(newNode);
    int doubleSuccessorNew = findReplicaNode2(successorNew);
    printf("%d %d %d\n", predecessorNew, successorNew, doubleSuccessorNew);

    if (vmIndex == predecessorNew) {
        splitList(successorNew, newNode);
        fastSet(newNode, predecessorNew, 3);
        deleteListAndData(successorNew);
    } else if (vmIndex == successorNew) {
        splitList(successorNew, newNode);
        fastSet(newNode, newNode, 3);
        deleteListAndData(predecessorNew);
    } else if (vmIndex == doubleSuccessorNew) {
        splitList(successorNew, newNode);
        fastSet(newNode, successorNew, 3);
        deleteListAndData(newNode);
    }
}

void splitList(int splitFrom, int splitTo) {
    unordered_set<string> fromList = dataListMap.find(splitFrom)->second;

    unordered_set<string> newFromList;
    unordered_set<string> newToList;

    int twoNodeList = (1 << splitFrom);
    twoNodeList |= (1 << splitTo);
    for (string key : fromList) {
        int masterNode = findMasterNode(key, twoNodeList);

        if (masterNode == splitTo) newToList.insert(key);
        else newFromList.insert(key);
    }

    dataListMap.find(splitFrom)->second = newFromList;
    dataListMap.find(splitTo)->second = newToList;
}

int findMasterNode(string key, int trustList_here) {
    // calculate key's hash
    int hashCode = key[0] % 10;

    // find the master node in trust list
    for (int i = 0; i < 10; i++) {
        int virtualIdx = i + hashCode;
        if (virtualIdx > 9) virtualIdx -= 10;

        if ((trustList_here & (1 << virtualIdx)) != 0) return virtualIdx;
    }
    return vmIndex;
}

void deleteListAndData(int toDelete) {
    unordered_set<string> deleteList = dataListMap.find(toDelete)->second;

    for (string key : deleteList) {
        storageMap.erase(storageMap.find(key));
    }

    unordered_set<string> emptyList;
    dataListMap.find(toDelete)->second = emptyList;
}

void* commandInput(void*) {
    while (!joinedIntoCluster) {
        usleep(10 * 1000);
    }

    bool flag = true;
    while (flag) {
        printf("[client] Input message: ");
        string commandStr;
        getline(cin,commandStr);

        // locked on recovery
        while (recoveryNodes > 0) {
            usleep(10 * 1000);
        }

        if(commandStr[0]=='!') {
            flag=false;
            
            // shutdown all alive server of itself
            for (int i = 0; i < vmNum; i++) {
                if (i != vmIndex) {
                    shutdown(client_send_socket[i], 2);
                    shutdown(server_recv_socket[i], 2);

                    close(client_send_socket[i]);
                    close(server_recv_socket[i]);
                }
            }

            printf("See you!\n");
        } else if (commandStr[0]=='v') {
            printf("version: %s\n", version_code.c_str());
        } else if (commandStr[0]=='r') {
            printf("replica confirmed: %d\n", replicaConfirm);
        } else if (commandStr[0]=='a') {
            cout<<"print list:"<<endl;
            for (int i = 0; i < 10; i++) {
                cout<<i<<": "<<endl;
                unordered_set<string> keys = dataListMap.find(i)->second;
                for (string key : keys) {
                    cout<<key<<' '<<storageMap.find(key)->second<<endl;
                }
            }
        } else if (commandStr[0]=='s') {
            cout<<"seed commands"<<endl;

            cout << preProcessCommand("SET def abc") << endl; // 0
            cout << preProcessCommand("SET dgh bcd") << endl;
            cout << preProcessCommand("SET fij code") << endl; // 3
            cout << preProcessCommand("SET fqp def") << endl;
            cout << preProcessCommand("SET abc efg") << endl; // 7
            cout << preProcessCommand("SET adef ugh") << endl;
            cout << preProcessCommand("SET cd ghi") << endl; // 9
            cout << preProcessCommand("SET cgh hij") << endl;
            cout << preProcessCommand("SET hij 123") << endl; // 5
            cout << preProcessCommand("SET hab 234") << endl;
        } else if (commandStr[0]=='t') {
            cout<<"trustList is";
            for (int i = 9; i >= 0; i--) {
                if ((trustList & (1 << i)) != 0) cout<<i<<' ';
            }
            cout<<endl;
        } else {
            // pre-process command
            cout << preProcessCommand(commandStr) << endl;
        }
                
        sleep(1);
    }
    
    pthread_exit(NULL);
}

string preProcessCommand(string commandStr) {
    // wait on joining
    while (!joinedIntoCluster) {
        usleep(1000);
    }

    // wait on recovery
    while (recoveryNodes > 0) {
        usleep(10 * 1000);
    }

    // split command string
    vector<string> commands = split(commandStr, ' ');

    if (commands.size() > 3) {
        printf("command not legal");
    }

    // check command type
    if(commands[0] == "SET") {
        // SET key value
        if (commands.size() == 3) {
            // broadcast to change node state to hasData
            if (!hasData) {
                hasData = true;

                for (int i = 0; i < 10; i++) {
                    if (i != vmIndex && (trustList & (1 << i)) != 0)
                        SendMessage(i, 11, "", "");
                }
            }
            

            int masterNode = findMasterNode(commands[1], trustList);

            cout<<"master node is "<<masterNode<<endl;

            if (masterNode == vmIndex) return setLocal(commands[1], commands[2], vmIndex);
            else {
                SendMessage(masterNode, 1, commands[1], commands[2]);

                // wait for reply
                while (!replyArrived) {
                    usleep(1000);
                }

                replyArrived = false;
                return replyBuffer;
            }
        }
    } else if(commands[0] == "GET") {
        // GET key
        if (commands.size() == 2) {
            int masterNode = findMasterNode(commands[1], trustList);

            cout<<"master node is "<<masterNode<<endl;

            if (masterNode == vmIndex) return getLocal(commands[1]);
            else {
                SendMessage(masterNode, 2, commands[1], "");

                // wait for reply
                while (!replyArrived) {
                    usleep(1000);
                }

                replyArrived = false;
                return replyBuffer;
            }
        }
    } else if(commands[0] == "OWNERS") {
        // OWNERS key
        if (commands.size() == 2) {
            int masterNode = findMasterNode(commands[1], trustList);

            cout<<"master node is "<<masterNode<<endl;

            if (masterNode == vmIndex) return ownersLocal(vmIndex);
            else {
                SendMessage(masterNode, 2, commands[1], "");

                // wait for reply
                while (!replyArrived) {
                    usleep(1000);
                }

                replyArrived = false;

                if (replyBuffer[0] == 'N') return "";
                else return ownersLocal(masterNode);
            }
        }
    } else if(commands[0] == "LIST_LOCAL") {
        // LIST_LOCAL
        if (commands.size() == 1)
            return listLocal();
    } else if(commands[0] == "BATCH") {
        // BATCH from to
        if (commands.size() == 3) {
            string file1 = commands[1];
            string file2 = commands[2];
            ifstream inputfile(file1.c_str());
            ofstream outputfile(file2.c_str());
            string tmp;

            bool inputFlag = inputfile.is_open();
            bool outputFlag = outputfile.is_open();

            cout<<"input"<<inputFlag<<endl;
            cout<<"output"<<outputFlag<<endl;

            if(inputFlag && outputFlag) {
                while(getline(inputfile, tmp)) {
                    string resultStr = preProcessCommand(tmp);

                    if(resultStr.length() == 0)
                        cout<<"Command Error"<<endl;
                    else {
                        cout<< resultStr << endl;
                        outputfile << resultStr << endl;
                    }

                }
            }

            if (inputFlag) inputfile.close();
            if (outputFlag) outputfile.close();

            return "";
        }
    }

    return "command not legal";
}

string setLocal(string key, string value, int masterNode) {
    storeDataAndUpdateList(key, value, masterNode);

    replicateData(key, value);
    return "SET OK";
}

void* setLocal_thread(void*) {
    string key;
    string value;
    int requestIdx;

    while (true) {
        // get key, value, requestIdx
        while (!keyQueue.empty()) {
            mtx_set_buffer.lock();
            key = keyQueue.front();
            value = valueQueue.front();
            requestIdx = requesterQueue.front();

            keyQueue.pop();
            valueQueue.pop();
            requesterQueue.pop();
            mtx_set_buffer.unlock();

            string set_result = setLocal(key, value, vmIndex);

            // send set result back
            SendMessage(requestIdx, 3, "", set_result);
        }

        usleep(1000);
    }
}

void storeDataAndUpdateList(string key, string value, int masterNode) {
    if (storageMap.find(key) == storageMap.end())
        storageMap.insert(std::make_pair(key,value));
    else
        storageMap.find(key)->second = value;

    dataListMap.find(masterNode)->second.insert(key);
}

string getLocal(string key) {
    if (storageMap.find(key) == storageMap.end())
        return "Not found";
    else
        return "Found: " + storageMap.find(key)->second;
}

string ownersLocal(int masterNode) {
    stringstream ansStrSS;
    ansStrSS << masterNode << " ";
    ansStrSS << findReplicaNode1(masterNode) << " ";
    ansStrSS << findReplicaNode2(masterNode);
    return ansStrSS.str();
}

string listLocal() {
    stringstream resultSS;
    for (const auto &myPair : storageMap) {
        resultSS << myPair.first << endl;
    }
    resultSS << "END LIST";

    return resultSS.str();
}

void* SendMessage(int receiver, int type, string key, string value) {
    // type: 0 exit; 1 set; 2 get; 3 set reply; 4 get reply; 5 replicate set; 6 set done in replicas; 
    // 7 length message; 8: fast set 9: local recovery complete 10: recovery complete 11: setData
    // 12: welcome message from node when node join (with trustlist attached) 
    // 13: leader node ask neighbor node to fast set on new node
    // 14: new node joined
    stringstream sendStrSS;
    switch(type) {
        case 0: 
            sendStrSS << 0;
            break;

        case 1: 
            sendStrSS << 1 << ' ' << key.length() << ' ' << key;
            sendStrSS << ' ' << value.length() << ' ' << value;
            break;

        case 2: 
            sendStrSS << 2 << ' ' << key.length() << ' ' << key;
            break;

        case 3: 
            sendStrSS << 3 << ' ' << value.length() << ' ' << value;
            break;

        case 4: 
            sendStrSS << 4 << ' ' << value.length() << ' ' << value;
            break;

        case 5: 
            sendStrSS << 5 << ' ' << key.length() << ' ' << key;
            sendStrSS << ' ' << value.length() << ' ' << value;
            break;

        case 6: 
            sendStrSS << 6 << ' ';
            break;

        case 8:
            sendStrSS << 8 << ' ' << value;
            break;

        case 9:
            sendStrSS << 9 << ' ' << key << ' ';
            break;

        case 10:
            sendStrSS << 10 << ' ';
            break;

        case 11:
            sendStrSS << 11 << ' ';
            break;

        case 12:
            sendStrSS << 12 << ' ' << key << ' ';
            break;

        case 13:
            sendStrSS << 13 << ' ';
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

        int res = send(client_send_socket[receiver], message_buffer, buffer_size, 0);
        if(res < 0) cout << "ERROR send (length message)" << endl;

        // send message part by part
        for (int i = 0; i < parts - 1; i++) {
            bzero(message_buffer, buffer_size);
            string partStr = sendStr.substr(i * (buffer_size - 10), (buffer_size - 10));
            strcpy(message_buffer, partStr.c_str());

            res = send(client_send_socket[receiver], message_buffer, buffer_size, 0);
            if(res < 0) cout << "ERROR send (parts message)" << endl;
        }

        bzero(message_buffer, buffer_size);
        strcpy(message_buffer, sendStr.substr((parts - 1) * (buffer_size - 10)).c_str());

        res = send(client_send_socket[receiver], message_buffer, buffer_size, 0);
        if(res < 0) cout << "ERROR send (last part)" << endl;
    } else {
        char message_buffer[buffer_size];
        bzero(message_buffer, buffer_size);
        strcpy(message_buffer, sendStrSS.str().c_str());

        int res = send(client_send_socket[receiver], message_buffer, buffer_size, 0);
        if(res < 0) cout << "ERROR send (single message)" << endl;
    }
}

bool replicateData(string key, string value) {
    int replicaNode1 = findReplicaNode1(vmIndex);
    int replicaNode2 = findReplicaNode2(vmIndex);

    replicaConfirm = 0;

    SendMessage(replicaNode1, 5, key, value);
    SendMessage(replicaNode2, 5, key, value);

    while (replicaConfirm < 2) {
        usleep(1000);
    }

    return true;
}

int findReplicaNode1(int masterNode) {
    // find the pre-replica node in trust list
    for (int i = 1; i < 10; i++) {
        int virtualIdx = -i + masterNode + 10;
        if (virtualIdx > 9) virtualIdx -= 10;

        if ((trustList & (1 << virtualIdx)) != 0) return virtualIdx;
    }
    return vmIndex;
}

int findReplicaNode2(int masterNode) {
    // find the post-replica node in trust list
    for (int i = 1; i < 10; i++) {
        int virtualIdx = i + masterNode;
        if (virtualIdx > 9) virtualIdx -= 10;

        if ((trustList & (1 << virtualIdx)) != 0) return virtualIdx;
    }
    return vmIndex;
}