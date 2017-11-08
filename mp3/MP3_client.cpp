//This is the code for the client (0-2).
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

vector<string> split(string str, char delimiter) {
    vector<string> internal;

    // segment 1
    int pos_seg1 = str.find(" ");
    if (pos_seg1 == std::string::npos || pos_seg1 < 0) {
        internal.push_back(str);
        return internal;
    }

    internal.push_back(str.substr(0, pos_seg1));
    internal.push_back(str.substr(pos_seg1 + 1, 1));
    
    // segment 2
    int pos_seg2 = str.find(" ", pos_seg1 + 3);
    if (pos_seg2 == std::string::npos || pos_seg2 < 0) {
        internal.push_back(str.substr(pos_seg1 + 3));
        return internal;
    }

    internal.push_back(str.substr(pos_seg1 + 3, pos_seg2 - pos_seg1 - 3));

    // segment 3
    internal.push_back(str.substr(pos_seg2 + 1));
  
    return internal;
}

/*	global variable definition	*/
string version_code = "c-0.4.1: deadlock false positive";
int sigErrorPrint;	// count number of sig error printed
int localTest = 2;	//default mode is test on vm
int serverNum = 5;	// number of vms
int clientNum = 3;
int vmIndex; // index of this vm (start from 0) 0-2 client, 3-7 servers, 8 coordinator
int serverRange = 50;
int portBase = 4000; // can be modified by user
int buffer_size = 512; // socket message buffer size
int server_recv_socket[10]; // server sockets
int client_send_socket[10]; // client sockets
bool abort_flag = false;	//verify the transaction to be aborted or committed
bool inTransaction = false;	// if the client has started a transaction, true
//bool commitlist[10] = {false}; //which server needs to be committed, if true, need, if false, not
int verbose = 0;
char server_char_list[5] = {'A', 'B', 'C', 'D', 'E'};
int mssgLengths[10]; // message length array
bool vm_up[10]; // whether the vm is up (passively detect)
int vmNum = 10;
int commit_ack = 0; // number of commit/abort ack
bool server_reply_received = true; // whether it has received reply from server

queue<string> mssgQueue;
queue<int> serverIdxQueue;

mutex mtx_commit;
//mutex mtx_set_buffer;
mutex mtx_send_array[10];

void* initializeServer(void*);
void* acceptConnection(void* param);
int processBuffer(string buffer, int serverIdx);
void* initializeClient(void*);
void* buildConnection(void* param);
void* commandInput(void*);
void* abort_thread(void*);
void processCommand(string commandStr);
void* SendMessage(int receiver, int type, string key, string value);

// clnt_node to svr_node: 0 SET; 1 GET; 2 COMMIT; 3 ABORT
// svr_node to clnt_node: 10 SET OK; 11 GET OK; 12 COMMITED 13 GET NOT FOUND

/*	Catch Signal Handler function	*/
void signal_callback_handler(int signum) {
    if (sigErrorPrint < 3) {
        fprintf(stderr,"[ERROR] sigpipe error!\n");
        sigErrorPrint++;
    }
}

/*	main processes defination	*/
int main(int argc, char *argv[])
{	
	if (argc < 1) 
	{
        fprintf(stderr,"[ERROR] not enough params provided\n");
        exit(1);
    }
    vmIndex = atoi(argv[1]);
    if (argc >= 3) 
    	localTest = atoi(argv[2]); 	//localTest = 1, localtest mode

    if (argc >= 4) verbose = atoi(argv[3]);

    // register SIGPIPE
    signal(SIGPIPE, signal_callback_handler);

    cerr<<"Client node mode\n";

    for (int i = 0; i < 10; i++) {
        vm_up[i] = true;
    }

    pthread_t serverThread, clientThread, commandThread;

    // create server threads
    int rc = pthread_create(&serverThread, NULL, initializeServer, NULL);
    if (rc)
    {
        std::cerr << "[Error] unable to create server thread," << rc << std::endl;
        exit(-1);
    }
    sched_yield();
    sleep(5);

    //	create client threads
    rc = pthread_create(&clientThread, NULL, initializeClient, NULL);
    if (rc){
        std::cerr << "[Error] unable to create thread," << rc << std::endl;
        exit(-1);
    }
    
    //	create user interface threads
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
    for (int j = 3; j <= 8; j++) {
        if (j == vmIndex) continue;

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
                        fprintf(stderr,"[LOG-2] [%ld]I've received some long message: %d\n", serverIdx, mssg_length);

                    for (int i = 0; i < mssg_length; i++) {
                        bzero(buffer,buffer_size);
                        res = recv(server_recv_socket[serverIdx], buffer, buffer_size, 0);

                        // in case sender fail at middle
                        if (res <= 0) {
                            type = -1;
                            fprintf(stderr, "[ERROR] sender fails at middle\n"); 
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

void* abort_thread(void*) {
    // report ABORT to servers
    for (int i = 3; i <= 7; i++) {
        SendMessage(i, 3, "", "");
    }

    while (commit_ack < 5) {
        usleep(1000);
    }

    commit_ack = 0;

    // report ABORT to coor
    SendMessage(8, 3, "", "");

    while (commit_ack < 1) {
        usleep(1000);
    }

    commit_ack = 0;

    //cout << "ABORT" << endl << flush;//

    inTransaction = false;
    server_reply_received = true;

    pthread_exit(NULL);
}

int processBuffer(string recv_message_str, int serverIdx){
	if (recv_message_str.length() < 1) {
        // what is this? E.T.?
        fprintf(stderr, "[ERROR] E.T.?\n");
        return -1;
    }

    int pos_mode = recv_message_str.find(" ");
    if (pos_mode == std::string::npos || pos_mode == 0) {
        fprintf(stderr, "[ERROR] Strange message?\n");
        return 100;
    }

    int mode = stoi(recv_message_str.substr(0, pos_mode));

    if (mode == 7) 
    {
        // get length
        int pos_mssg_length = recv_message_str.find(" ", pos_mode + 1);
        int mssg_length = stoi(recv_message_str.substr(pos_mode + 1, pos_mssg_length - pos_mode - 1));

        // store length into lengths array
        mssgLengths[serverIdx] = mssg_length;

        return 7;
    }
	else if(mode == 10) {// mode 10 : SET is done, OK
		cout << "OK" << endl << flush;

        server_reply_received = true;

        return 10;
	}
    else if(mode == 11)	// mode 11 : GET IS RETURNED WITH A VALUE
	{
		// key
        int pos_key = recv_message_str.find(" ", pos_mode + 1);
        int key_size = stoi(recv_message_str.substr(pos_mode + 1, pos_key - pos_mode - 1));
        string key = recv_message_str.substr(pos_key + 1, key_size);

        // value
        int pos_value = recv_message_str.find(" ", pos_key + 1 + key_size + 1);
        int value_size = stoi(recv_message_str.substr(pos_key + 1 + key_size + 1, pos_value - pos_key - 1));
        string value = recv_message_str.substr(pos_value + 1, value_size);

		cout << server_char_list[serverIdx - clientNum] << "." << key << " = " << value << endl << flush;

        server_reply_received = true;

        return 11;
	}
	else if(mode == 12)	// mode 12: Commit operation is done in this server
	{
		//commitlist[serverIdx - 3] = false;
        mtx_commit.lock();
        commit_ack++;
        mtx_commit.unlock();

        return 12;
	}
	else if(mode == 13)	//	mode 13: GET do not have a result, so abort
	{
		cout << "NOT FOUND" << endl << flush;

        pthread_t abortThread;
        int rc = pthread_create(&abortThread, NULL, abort_thread, NULL);
        if (rc){
            std::cerr << "[Error] unable to create thread," << rc << std::endl;
            exit(-1);
        }

        return 13;
	}
    else if(mode == 26) //  mode 26: deadlock, so abort
    {
        cout << "ABORT" << endl << flush; // 1 DEADLOCK = ABORT

        pthread_t abortThread;
        int rc = pthread_create(&abortThread, NULL, abort_thread, NULL);
        if (rc){
            std::cerr << "[Error] unable to create thread," << rc << std::endl;
            exit(-1);
        }

        return 26;
    }

    return -1;
}

void* initializeClient(void*){
    pthread_t messageThreads[vmNum];
    long idxes[vmNum];
        
    // when it is its turn to connect
    for (int j = 3; j <= 8; j++) {
        if (j == vmIndex) continue;

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

//	commandInput = user interface thread
void* commandInput(void*){
	bool flag = true;
	while(flag)
	{
        while (!server_reply_received) {
            usleep(1000);
        }

        cerr<<"Input: ";

		string commandStr;
		getline(cin,commandStr);

		if (commandStr[0]=='v') {
            fprintf(stderr,"version: %s\n", version_code.c_str());
        } else processCommand(commandStr);

		usleep(1000);
	}
	pthread_exit(NULL);
}

//	preprocess the user's input command
void processCommand(string commandStr){
	vector<string> commands = split(commandStr, ' ');

	if(commands[0] == "BEGIN" && inTransaction == false)
	{
		inTransaction = true;
        cout << "OK" << endl << flush;

        return ;
	}
	else if(inTransaction)
	{
		if(commands[0] == "SET")
		{
            if (commands.size() == 4) {
                server_reply_received = false;

                int serverIdx = commands[1][0] - 'A' + clientNum;
                //commitlist[serverIdx] = true;
                SendMessage(serverIdx, 0, commands[2],commands[3]); //  mode 0: send set message

                return ;
            }
		}
		else if(commands[0] == "GET")
		{
            if (commands.size() == 3) {
                server_reply_received = false;

                int serverIdx = commands[1][0] - 'A' + clientNum;
                //commitlist[serverIdx] = true;
                SendMessage(serverIdx, 1, commands[2], ""); //  mode 1: send get message

                return ;
            }
		}
        else if(commands[0] == "COMMIT")
        {
            server_reply_received = false;

            for (int i = 3; i <= 7; i++) {
                SendMessage(i, 2, "", "");
            }

            while (commit_ack < 5) {
                usleep(1000);
            }

            commit_ack = 0;

            // report COMMIT to coor
            SendMessage(8, 2, "", "");

            while (commit_ack < 1) {
                usleep(1000);
            }

            commit_ack = 0;

            cout<<"COMMIT OK"<<endl<<flush;

            inTransaction = false;
            server_reply_received = true;
            return ;
        }
        else if(commands[0] == "ABORT")
        {
            server_reply_received = false;

            for (int i = 3; i <= 7; i++) {
                SendMessage(i, 3, "", "");
            }

            while (commit_ack < 5) {
                usleep(1000);
            }

            commit_ack = 0;

            // report ABORT to coor
            SendMessage(8, 3, "", "");

            while (commit_ack < 1) {
                usleep(1000);
            }

            commit_ack = 0;

            cout<<"ABORT"<<endl<<flush;//

            inTransaction = false;
            server_reply_received = true;
            return ;
        }
        else
        {
            cerr << "[ERROR] WRONG INPUT" << endl;
        }
	}
    else
    {
        cerr << "[ERROR] WRONG INPUT" << endl;
    }
    
}

void* SendMessage(int receiver, int type, string key, string value) {
	stringstream sendStrSS;
	switch(type)
	{
		case 0:	// mode 0 : SET value key
			sendStrSS << 0 << ' ' << key.length() << ' ' << key << ' ' << value.length() << ' ' << value;
			break;
		case 1:	//	mode 1: GET key
			sendStrSS << 1 << ' ' << key.length() << ' ' << key;
			break;
		case 2:	//	mode 2: COMMIT
			sendStrSS << 2 << ' ';
			break;
		case 3:	// 	mode 3: ABORT
			sendStrSS << 3 << ' ';
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
                fprintf(stderr, "[LOG-2] partStr[%d] : %s\n", i, partStr.c_str());

            strcpy(message_buffer, partStr.c_str());

            res = send(client_send_socket[receiver], message_buffer, buffer_size, 0);
            if(res < 0) cerr << "[ERROR] send (parts message)" << endl;

            usleep(1000); // v1
        }

        bzero(message_buffer, buffer_size);
        strcpy(message_buffer, sendStr.substr((parts - 1) * (buffer_size - 10)).c_str());

        if (verbose > 1)
            fprintf(stderr, "[LOG-2] partStr[last] : %s\n", sendStr.substr((parts - 1) * (buffer_size - 10)).c_str());

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
