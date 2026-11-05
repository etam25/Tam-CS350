#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include "common.h"

#define BACKLOG_COUNT 100
#define USAGE_STRING  \
    "Missing parameter. Exiting.\n" \
    "Usage: %s <port_number>\n"

static void handle_connection(int conn_socket) {
    uint64_t prev_request_id = 0;  // track the previous request ID
    while (1) {
        struct request req;
        struct response res;

        // receive the request from the client
        ssize_t bytes_received = recv(conn_socket, &req, sizeof(req), 0);
        if (bytes_received <= 0) {
            if (bytes_received < 0) perror("recv");
            break; // connection closed or error
        }

        // record the receipt timestamp
        struct timespec receipt_timestamp;
        clock_gettime(CLOCK_MONOTONIC, &receipt_timestamp);

        // perform a busy wait for the request length
        get_elapsed_busywait(req.request_length.tv_sec, req.request_length.tv_nsec);

        // record the completion timestamp
        struct timespec completion_timestamp;
        clock_gettime(CLOCK_MONOTONIC, &completion_timestamp);

        // prepare the response
        res.request_id = req.request_id;
        res.response = (req.request_id == prev_request_id + 1) ? 0 : 0x0F;
        res.ack = 0;

        // send the response back to the client
        send(conn_socket, &res, sizeof(res), 0);

        // output the request information
        printf("R%lu:%.6f,%.6f,%.6f,%.6f\n",
               req.request_id,
               TSPEC_TO_DOUBLE(req.sent_timestamp),
               TSPEC_TO_DOUBLE(req.request_length),
               TSPEC_TO_DOUBLE(receipt_timestamp),
               TSPEC_TO_DOUBLE(completion_timestamp));

        // update the previous request ID
        prev_request_id = req.request_id;
    }

    close(conn_socket);
}

int main(int argc, char **argv) {
    int sockfd, retval, accepted, optval;
    in_port_t socket_port;
    struct sockaddr_in addr, client;
    socklen_t client_len;

    /* get port to bind our socket to */
    if (argc > 1) {
        socket_port = strtol(argv[1], NULL, 10);
        printf("INFO: setting server port as: %d\n", socket_port);
    } else {
        fprintf(stderr, USAGE_STRING, argv[0]);
        return EXIT_FAILURE;
    }

    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("Unable to create socket");
        return EXIT_FAILURE;
    }

    optval = 1;
    setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, (void *)&optval, sizeof(optval));

    addr.sin_family = AF_INET;
    addr.sin_port = htons(socket_port);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);

    retval = bind(sockfd, (struct sockaddr *)&addr, sizeof(struct sockaddr_in));
    if (retval < 0) {
        perror("Unable to bind socket");
        return EXIT_FAILURE;
    }

    retval = listen(sockfd, BACKLOG_COUNT);
    if (retval < 0) {
        perror("Unable to listen on socket");
        return EXIT_FAILURE;
    }

    printf("INFO: Using timelib version %s\n", TIMELIB_VER);
    printf("INFO: Waiting for incoming connection...\n");

    client_len = sizeof(struct sockaddr_in);
    accepted = accept(sockfd, (struct sockaddr *)&client, &client_len);
    if (accepted == -1) {
        perror("Unable to accept connections");
        return EXIT_FAILURE;
    }

    handle_connection(accepted);

    close(sockfd);
    return EXIT_SUCCESS;
}