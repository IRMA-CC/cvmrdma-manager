#include "tcp.h"

int parse_client_address(const char* input, struct sockaddr_in* out) {
    char ip[64];
    char port_str[16];

    // Split at ':'
    const char* colon = strchr(input, ':');
    if (!colon) return -1;

    size_t ip_len = colon - input;
    if (ip_len >= sizeof(ip)) return -1;

    memcpy(ip, input, ip_len);
    ip[ip_len] = '\0';

    strcpy(port_str, colon + 1);

    int port = atoi(port_str);
    if (port <= 0 || port > 65535) return -1;

    out->sin_family = AF_INET;
    out->sin_port = htons(port);

    if (inet_pton(AF_INET, ip, &out->sin_addr) != 1)
        return -1;

    return 0;
} 

// send a tcp_client_msg to a remote instance of the cvmrdma_manager
// the remote manager will then handle routing the message to the correct libibverbs instance
int connect_to_server(char server_ip[64], struct tcp_client_msg* message) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) { fprintf(stderr, "Tcp Client: socket error"); exit(1); }

    struct sockaddr_in server_addr;
    if (parse_client_address(server_ip, &server_addr) != 0) {
        fprintf(stderr, "Invalid server address: %s\n", server_ip);
        exit(1);
    }

    if (connect(sock, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        fprintf(stderr, "Tcp Client: Failed to connect to %s\n", server_ip);
        exit(1);
    }

    fprintf(stderr, "Tcp Client: Sending message to %s\n", server_ip);
    send_all(sock, message, sizeof(*message));

    close(sock);
    return 0;
}