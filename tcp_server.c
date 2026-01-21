#include "tcp.h"

// Global control flag
atomic_int server_running = 1;
// Server socket (needed so stop_server() can shut it down)
int server_fd = -1;

struct qp_entry {
    uint32_t qpn;
    int socket;
};

#define MAX_QPS 1024
struct qp_entry qp_table[MAX_QPS];
int num_qps = 0;
static pthread_mutex_t qp_table_mutex = PTHREAD_MUTEX_INITIALIZER;

// only needed in one function to avoid deadlock, should not be used
static int lookup_qp_no_lock(uint32_t qpn) {
    for (int i = 0; i < num_qps; i++) {
        if (qp_table[i].qpn == qpn) {
            return qp_table[i].socket;
        }
    }
    return -1;
}

// checks if the qpn exists and if yes, return its associated posix socket
int lookup_qp(uint32_t qpn) {
     pthread_mutex_lock(&qp_table_mutex);
    for (int i = 0; i < num_qps; i++) {
        if (qp_table[i].qpn == qpn) {
            int s = qp_table[i].socket;
            pthread_mutex_unlock(&qp_table_mutex);
            return s;
        }
    }
    pthread_mutex_unlock(&qp_table_mutex);
    return -1;
}

int parse_server_address(const char* input, struct sockaddr_in* out) {
    char port_str[16];

    const char* colon = strchr(input, ':');
    if (!colon) return -1;

    strcpy(port_str, colon + 1);
    int port = atoi(port_str);
    if (port <= 0 || port > 65535) return -1;

    out->sin_family = AF_INET;
    out->sin_port = htons(port);

    if (strncmp(input, "*", 1) == 0) {
        out->sin_addr.s_addr = INADDR_ANY;
    } else {
        fprintf_time_stderr(stderr, "Tcp Server: Specified IP address is not supported, use '*'\n");
        return -1;
    }

    return 0;
}

int send_tcp_client_msg(struct tcp_client_msg msg) {
    // it can sometimes happen that we receive a message from a remote, before the local associated
    // qpn was registered. 
    // try to poll for it and time out after 100 retries
    int socket = lookup_qp(msg.qpn);
    int retrys = 0;
    while (socket < 0) {
        usleep(100000); // Sleep for 100ms if no qpn
        socket = lookup_qp(msg.qpn);
        retrys++;
        if (retrys > 100) {
            fprintf_time_stderr(stderr, "Missing QPN %u, closing connection\n", msg.qpn);
            return -1;
        }
    }

    // fprintf_time_stderr(stderr, "Tcp Server: Sending message for QPN %u to local libibverbs instance\n", msg.qpn);

    // send the message from the remote to the local libibverbs instance of the associated qp
    fprintf_time_stderr(stderr, "Received key from QPN %u message to QPN %u using socket %d\n", msg.send_qp, msg.qpn, socket);
    int bytes_written = send_all(socket, &msg, sizeof(msg));
    if (bytes_written < 0) {
        fprintf_time_stderr(stderr, "Tcp Server: write failed\n");
    }

    // Shutdown write end to signal we're done sending, but allow receiver to finish reading
    shutdown(socket, SHUT_WR);
    
    // Optional: wait for the receiver to close their end (read until EOF)
    // char dummy;
    // while (recv(socket, &dummy, 1, 0) > 0);

    // fprintf_time_stderr(stderr, "Tcp Server: Sent message for QPN %u to local libibverbs instance\n", msg.qpn);

    return 0;
}

// -------------------------------------------------------
//          Gracefully stop the server thread
// -------------------------------------------------------
void stop_server(pthread_t* thread) {
    atomic_store(&server_running, 0);

    // This unblocks accept()
    if (server_fd >= 0)
        shutdown(server_fd, SHUT_RDWR);

    pthread_join(*thread, NULL);
    if (server_fd >= 0) {
        close(server_fd);
        server_fd = -1;
    }
}

// -------------------------------------------------------
//                Server thread
// -------------------------------------------------------
void* server_loop(void* arg) {
    int fd = *(int*)arg;

    while (atomic_load(&server_running)) {
        int client_fd = accept(fd, NULL, NULL);

        if (client_fd < 0) {
            // If stopping, accept will fail after shutdown()
            if (!atomic_load(&server_running))
                break;

            fprintf_time_stderr(stderr, "Tcp Server: accept failed\n");
            continue;
        }
        
        struct tcp_client_msg msg;
        recv_all(client_fd, &msg, sizeof(msg));

        send_tcp_client_msg(msg);

        close(client_fd);
    }

    fprintf_time_stderr(stderr, "Tcp Server: Server loop exited.\n");
    return NULL;
}

// -------------------------------------------------------
//                    Start server
// -------------------------------------------------------
void start_server(pthread_t* thread, const char* bind_addr) {

    struct sockaddr_in addr;
    if (parse_server_address(bind_addr, &addr) != 0) {
        fprintf_time_stderr(stderr, "Tcp Server: Invalid bind address: %s\n", bind_addr);
        exit(1);
    }

    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) { fprintf_time_stderr(stderr, "Tcp Server: socket"); exit(1); }

    if (bind(server_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        fprintf_time_stderr(stderr, "Tcp Server: bind failed\n");
        exit(1);
    }

    // start listening for incoming connections
    if (listen(server_fd, 10) < 0) {
        fprintf_time_stderr(stderr, "Tcp Server: listen failed\n");
        exit(1);
    }

    fprintf_time_stderr(stderr, "Tcp Server: Server listening on %s\n", bind_addr);

    // start thread to poll for connections and handle incoming messages
    int err = pthread_create(thread, NULL, server_loop, &server_fd);
    if (err != 0) {
        fprintf_time_stderr(stderr, "Tcp Server: pthread_create failed\n");
        exit(1);
    }
}

// -------------------------------------------------------
//                       Register QP
// -------------------------------------------------------
void register_qp_with_server(uint32_t qpn, int socket) {
    pthread_mutex_lock(&qp_table_mutex);
    if (num_qps >= MAX_QPS) {
        pthread_mutex_unlock(&qp_table_mutex);
        fprintf_time_stderr(stderr, "Tcp Server: Max QPs reached\n");
        exit(1);
    }
    if (lookup_qp_no_lock(qpn) != -1) { // lookup_qp locks, but fine for simplicity
        pthread_mutex_unlock(&qp_table_mutex);
        fprintf_time_stderr(stderr, "Tcp Server: QP with QPN %u already registered\n", qpn);
        exit(1);
    }
    fprintf_time_stderr(stderr, "Tcp Server: Registering QPN %u with socket %d\n", qpn, socket);
    qp_table[num_qps].qpn = qpn;
    qp_table[num_qps].socket = socket;
    num_qps++;
    pthread_mutex_unlock(&qp_table_mutex);

    // fprintf_time_stderr(stderr, "Tcp Server: Registered QPN %u\n", qpn);
}

void unregister_qp_from_server(uint32_t qpn) {

    pthread_mutex_lock(&qp_table_mutex);
    for (int i = 0; i < num_qps; i++) {
        if (qp_table[i].qpn == qpn) {
            for (int j = i; j < num_qps - 1; j++) {
                qp_table[j] = qp_table[j + 1];
            }
            num_qps--;
            pthread_mutex_unlock(&qp_table_mutex);
            return;
        }
    }
    pthread_mutex_unlock(&qp_table_mutex);

    // fprintf_time_stderr(stderr, "Tcp Server: No registered QP with QPN %u to unregister\n", qpn);
}

