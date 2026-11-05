#define _GNU_SOURCE 
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <sched.h>
#include <signal.h>
#include <pthread.h>
#include <time.h>
#include <unistd.h>

#include <sys/types.h>
#include <sys/wait.h>

#include "timelib.h"

#include "common.h"

#define BACKLOG_COUNT 100
#define USAGE_STRING \
    "Missing parameter. Exiting.\n" \
    "Usage: %s <port_number>\n"

/* 4KB of stack for the worker thread */
#define STACK_SIZE (4096)

/* Main logic of the worker thread 

    IMPLEMENT HERE THE MAIN FUNCTION OF THE WORKER */

/* 
- want worker thread to output: [#WORKER#] <timestamp> Worker Thread Alive! 
- then will enter a loop that does (implement in handle_connection?):
1. busywait for exactly 1 second
2. print: [#WORkER#]<timestamp> Still Alive!
3. sleep for 1 second 
*/
void* worker_thread(void* arg) {
    struct timespec current_time;

    clock_gettime(CLOCK_MONOTONIC, &current_time);
    printf("[#WORKER#] %.6f Worker Thread Alive!\n", TSPEC_TO_DOUBLE(current_time));

    while(1) {
        // busy wait for 1 second
        struct timespec start_time;
        clock_gettime(CLOCK_MONOTONIC, &start_time);
        while(1) {
            struct timespec now;
            clock_gettime(CLOCK_MONOTONIC, &now);
            if (TSPEC_TO_DOUBLE(now) - TSPEC_TO_DOUBLE(start_time) >= 1.0) {
                break;
            }
        }
        clock_gettime(CLOCK_MONOTONIC, &current_time);
        printf("[#WORKER#] %.6f Still Alive!\n", TSPEC_TO_DOUBLE(current_time));

        sleep(1);
    }

    return NULL;

    
} 

void handle_connection(int conn_socket) { 
    /* IMPLEMENT HERE THE LOGIC TO START THE WORKER THREAD */
    pthread_t thread_id;
       
    if (pthread_create(&thread_id, NULL, worker_thread, NULL) != 0) {
        perror("pthread_create failed");
        close(conn_socket);
        return;
    }

    pthread_detach(thread_id);

    // reuse logic from hw1 to handle the packets: 
// ( might not need all of it)
    uint64_t prev_request_id = 0; 
    while (1) { 
        struct request req; 
        struct response res;

        ssize_t bytes_received = recv(conn_socket, &req, sizeof(req), 0);
        if (bytes_received <= 0) {
            if (bytes_received < 0) perror("recv");
            break;
        }

        struct timespec receipt_timestamp;
        clock_gettime(CLOCK_MONOTONIC, &receipt_timestamp);

        get_elapsed_busywait(req.request_length.tv_sec, req.request_length.tv_nsec);

        struct timespec completion_timestamp;
        clock_gettime(CLOCK_MONOTONIC, &completion_timestamp);

        res.request_id = req.request_id;
        res.response = (req.request_id == prev_request_id + 1) ? 0 : 0x0F;
        res.ack = 0;

        send(conn_socket, &res, sizeof(res), 0);

       printf("R%lu:%.6f,%.6f,%.6f,%.6f\n", 
            req.request_id,
            TSPEC_TO_DOUBLE(req.sent_timestamp),
            TSPEC_TO_DOUBLE(req.request_length),
            TSPEC_TO_DOUBLE(receipt_timestamp),
            TSPEC_TO_DOUBLE(completion_timestamp)    
        );

        prev_request_id = req.request_id;

    }

    close(conn_socket);
}

int main (int argc, char ** argv) {
    int sockfd, retval, accepted, optval;
    in_port_t socket_port;
    struct sockaddr_in addr, client;
    struct in_addr any_address;
    socklen_t client_len;

    /* get port to bind our socket to */
    if (argc > 1) {
        socket_port = strtol(argv[1], NULL, 10);
        printf("INFO: setting server port as: %d\n", socket_port);
    } else {
        ERROR_INFO();
        fprintf(stderr, USAGE_STRING, argv[0]);
        return EXIT_FAILURE;
    }

    /* create the right type of socket */
    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        ERROR_INFO();
        perror("Unable to create socket");
        return EXIT_FAILURE;
    }

    /* before moving forward, set socket to reuse address */
    optval = 1;
    setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, (void *)&optval, sizeof(optval));

    /* convert INADDR_ANY into network byte order */
    any_address.s_addr = htonl(INADDR_ANY);

    /* time to bind the socket to the right port */
    addr.sin_family = AF_INET;
    addr.sin_port = htons(socket_port);
    addr.sin_addr = any_address;

    /* attempt to bind the socket with the given parameters */
    retval = bind(sockfd, (struct sockaddr *)&addr, sizeof(struct sockaddr_in));

    if (retval < 0) {
        ERROR_INFO();
        perror("Unable to bind socket");
        return EXIT_FAILURE;
    }

    /* set the server to listen on teh selected port */
    retval = listen(sockfd, BACKLOG_COUNT);

    if (retval < 0) {
        ERROR_INFO();
        perror("Unable to listen on socket");
        return EXIT_FAILURE;
    }

    /* ready to accept connections */
    printf("INFO: Using timelib version %s\n", TIMELIB_VER);
    printf("INFO: Waiting for incoming connectino...\n");
    client_len = sizeof(struct sockaddr_in);
    accepted = accept(sockfd, (struct sockaddr *)&client, &client_len);

    if (accepted == -1) {
        ERROR_INFO();
        perror("Unable to accept connections");
        return EXIT_FAILURE;
    }

    handle_connection(accepted);

    close(sockfd);
    return EXIT_SUCCESS;


}

