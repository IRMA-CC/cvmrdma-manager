#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <pthread.h>
#include "tcp.h"

#define SOCKET_PATH "/tmp/cvmrdma_manager_socket"
#define MAX_CLIENTS 1024

int client_sockets[MAX_CLIENTS];
int client_count = 0;
pthread_mutex_t client_mutex = PTHREAD_MUTEX_INITIALIZER;

// this handles sends between the libibvers library and the cvmrdma_manager over posix sockets.
// clients can pose different commands to the server.
void* client_poll_thread(void* arg) {
    fd_set readfds;

    while (1) {
        FD_ZERO(&readfds);
        pthread_mutex_lock(&client_mutex);
        int max_fd = -1;
        for (int i = 0; i < client_count; i++) {
            FD_SET(client_sockets[i], &readfds);
            if (client_sockets[i] > max_fd)
                max_fd = client_sockets[i];
        }
        pthread_mutex_unlock(&client_mutex);

        if (max_fd < 0) {
            usleep(100000); // Sleep for 100ms if no clients
            continue;
        }

        // Wait until any client has data
        int ready = select(max_fd + 1, &readfds, NULL, NULL, NULL);
        if (ready < 0) { fprintf(stderr, "Manager: select error"); continue; }

        pthread_mutex_lock(&client_mutex);
        for (int i = 0; i < client_count; i++) {
            if (FD_ISSET(client_sockets[i], &readfds)) {
                struct manager_wr wr;
                int rc = recv_all(client_sockets[i], &wr, sizeof(wr));

                if (rc < 0) {
                    fprintf(stderr, "Manager: Client fd %d disconnected\n", client_sockets[i]);
                    close(client_sockets[i]);
                    for (int j = i; j < client_count - 1; j++) {
                        client_sockets[j] = client_sockets[j + 1];
                    }
                    client_count--;
                    i--;
                    continue;
                }

                switch (wr.type) {
                    case MANAGER_WR_REGISTER_QP: {
                        fprintf(stderr, "Manager: Register QP %u\n", wr.u.qpn.qpn);
                        register_qp_with_server(wr.u.qpn.qpn, client_sockets[i]);
                        break;
                    }
                    case MANAGER_WR_UNREGISTER_QP: {
                        fprintf(stderr, "Manager: Unregister QP %u\n", wr.u.qpn.qpn);
                        unregister_qp_from_server(wr.u.qpn.qpn);
                        close(client_sockets[i]);
                        // Remove from client_sockets
                        for (int j = i; j < client_count - 1; j++) {
                            client_sockets[j] = client_sockets[j + 1];
                        }
                        client_count--;
                        i--; // Adjust index after removal

                        break;
                    }
                    case MANAGER_WR_SEND_KEY: {
                        fprintf(stderr, "Manager: Send key for QP %u to %s\n", wr.u.tcp.msg.qpn, wr.u.tcp.server_ip);
                        connect_to_server(wr.u.tcp.server_ip, &wr.u.tcp.msg);
                        break;
                    }
                    default:
                        fprintf(stderr, "Manager: Unknown WR type %d\n", wr.type);
                }
            }
        }
        pthread_mutex_unlock(&client_mutex);
    }
    return NULL;
}

int main () {

    fprintf(stderr, "Manager: cvmrdma_manager started\n");

    int server_fd;
    struct sockaddr_un addr;

    server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (server_fd < 0) { fprintf(stderr, "Manager: socket error"); exit(1); }

    unlink(SOCKET_PATH);
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, SOCKET_PATH, sizeof(addr.sun_path)-1);

    if (bind(server_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) { fprintf(stderr, "Manager: bind error"); exit(1); }
    if (listen(server_fd, MAX_CLIENTS) < 0) { fprintf(stderr, "Manager: listen error"); exit(1); }

    // start tcp server which is part of the cvmrdma_manager
    pthread_t tcp_thread;
    start_server(&tcp_thread, "*:3456");
    // Start polling thread for messages send over posix sockets
    pthread_t thread;
    pthread_create(&thread, NULL, client_poll_thread, NULL);

    // Accept libibverbs clients on the same machine over posix sockets
    while (1) {
        int client_fd = accept(server_fd, NULL, NULL);
        if (client_fd < 0) { fprintf(stderr, "Manager:  accept error"); continue; }

        pthread_mutex_lock(&client_mutex);
        if (client_count < MAX_CLIENTS) {
            client_sockets[client_count++] = client_fd;
            fprintf(stderr, "Manager: Client connected, total: %d\n", client_count);
        } else {
            close(client_fd);
        }
        pthread_mutex_unlock(&client_mutex);
    }

    close(server_fd);
    unlink(SOCKET_PATH);
    return 0;

}