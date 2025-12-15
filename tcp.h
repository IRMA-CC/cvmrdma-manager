#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdatomic.h>

struct tcp_client_msg {
    uint32_t qpn;
    uint32_t local_ring_rkey;
    uint32_t control_rkey;
    uint32_t control_qpn;
    uint32_t control_psn;
};

enum manager_wr_type {
    MANAGER_WR_REGISTER_QP,
    MANAGER_WR_UNREGISTER_QP,
    MANAGER_WR_SEND_KEY
};

struct manager_wr_qpn {
    uint32_t qpn;
};

struct manager_wr_tcp {
    char server_ip[64];
    struct tcp_client_msg msg;
};

struct manager_wr {
    enum manager_wr_type type;
    union {
        struct manager_wr_qpn qpn;
        struct manager_wr_tcp tcp;
    } u;
};


void start_server(pthread_t* thread, const char* bind_addr);
void stop_server(pthread_t* thread);
void register_qp_with_server(uint32_t qpn, int socket);
void unregister_qp_from_server(uint32_t qpn);
int connect_to_server(char server_ip[64], struct tcp_client_msg* message);

int send_tcp_client_msg(struct tcp_client_msg msg);

static int send_all(int fd, const void *buf, size_t len) {
    const unsigned char *p = buf;
    while (len) {
        ssize_t n = send(fd, p, len, 0);
        if (n < 0) return -1;
        if (n == 0) return -1; // should not happen on blocking stream
        p += n;
        len -= (size_t)n;
    }
    return 0;
}

static int recv_all(int fd, void *buf, size_t len) {
    unsigned char *p = buf;
    while (len) {
        ssize_t n = recv(fd, p, len, 0);
        if (n < 0) return -1;
        if (n == 0) return -1; // peer closed early
        p += n;
        len -= (size_t)n;
    }
    return 0;
}