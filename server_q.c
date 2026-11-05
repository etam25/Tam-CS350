#define _GNU_SOURCE
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <sched.h>
#include <signal.h>

#include <sys/types.h>
#include <sys/wait.h>

#include <semaphore.h>
#include <pthread.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "timelib.h"
#include "common.h"

#define BACKLOG_COUNT 100
#define USAGE_STRING \
    "Missing parameter. Exiting.\n" \
    "Usage: %s <port_number>\n"

#define STACK_SIZE (4096)

// START - variables needed to protect the shared queue
sem_t * queue_mutex;
sem_t * queue_notify;
// END

#define QUEUE_SIZE 500

// one slot in the queue: the request plus the time the parent recv'd it
struct queue_entry {
    struct request req;
    struct timespec receipt_timestamp;
};

// circular buffer of requests shared between parent and worker
struct queue {
    /* IMPLEMENT ME */
    struct queue_entry entries[QUEUE_SIZE];
    int head;   // index of the next entry to remove
    int tail;   // index where the next entry will be inserted
    int count;  // number of entries currently in the queue

    // set by the parent right before add_to_queue(...)
    struct timespec next_receipt;
    // set by get_from_queue(...) for the entry it just returned
    struct timespec last_receipt;
    int last_valid; // 1 if the last get_from_queue(...) returned a real request
};

// parameters handed to the worker thread
struct WORKER_PASS {
    /* IMPLEMENT ME */
    int conn_socket;
    struct queue * the_queue;
    volatile int worker_done; // termination flag asserted by the parent
};

// add a new request <request> to the shared queue <the_queue>
int add_to_queue(struct request to_add, struct queue * the_queue) {
    int retval = 0; 
    /* QUEUE PROTECTION INTRO START */
    sem_wait(queue_mutex);
    /* QUEUE PROTECTION INTRO END */

    /* write code here
    - make sure to not return without going through the outro code
    */
    if (the_queue->count >= QUEUE_SIZE) {
        // queue is full: reject the request
        retval = -1;
    } else {
        the_queue->entries[the_queue->tail].req = to_add;
        the_queue->entries[the_queue->tail].receipt_timestamp = the_queue->next_receipt;
        the_queue->tail = (the_queue->tail + 1) % QUEUE_SIZE;
        the_queue->count++;
        retval = 0;
    }

    /* QUEUE PROTECTION OUTRO START*/
    sem_post(queue_mutex);
    sem_post(queue_notify);
    /* QUEUE PROTECTION OUTRO END*/
    return retval;
}

// add a new request <request> to the shared queue <the_queue>
struct request get_from_queue(struct queue * the_queue) {
    struct request retval;

    /* QUEUE PROTECTION INTRO START */
    sem_wait(queue_notify);
    sem_wait(queue_mutex);
    /* QUEUE PROTECTION INTRO END */

    /* write code here 
    - make sure not to return without going through thr outro code
    */
    if (the_queue->count == 0) {
        // only happens when woken up for termination (or after a rejected add)
        memset(&retval, 0, sizeof(retval));
        the_queue->last_valid = 0;
    } else {
        retval = the_queue->entries[the_queue->head].req;
        the_queue->last_receipt = the_queue->entries[the_queue->head].receipt_timestamp;
        the_queue->last_valid = 1;
        the_queue->head = (the_queue->head + 1) % QUEUE_SIZE;
        the_queue->count--;
    }

    /* QUEUE PROTECTION OUTRO START*/
    sem_post(queue_mutex);
    /* QUEUE PROTECTION OUTRO END */

    return retval;
}

/* Implement this method to correctly dump the status of teh queue 
   following the format: Q:[R<request ID>, R<request ID>, ...]
*/
void dump_queue_status(struct queue * the_queue) {
    int i;
    /* QUEUE PROTECTION INTRO START */
    sem_wait(queue_mutex);
    /* QUEUE PROTECTION INTRO END */

    // write code here
    printf("Q:[");
    for (i = 0; i < the_queue->count; i++) {
        int idx = (the_queue->head + i) % QUEUE_SIZE;
        printf("R%lu", (unsigned long)the_queue->entries[idx].req.request_id);
        if (i < the_queue->count - 1) {
            printf(",");
        }
    }
    printf("]\n");

    /* QUEUE PROTECTION OUTRO START */
    sem_post(queue_mutex);
    /* QUEUE PROTECTION OUTRO END */
}

/* main logic of the worker thread */
/* implement here the main function of the worker function */
void * worker_main(void * arg) {
    struct WORKER_PASS * params = (struct WORKER_PASS *)arg;
    struct queue * the_queue = params->the_queue;
    struct request req;
    struct response resp;
    struct timespec receipt_timestamp, start_timestamp, completion_timestamp;

    while (!params->worker_done) {
        // blocks until the parent adds something (or asks us to terminate)
        req = get_from_queue(the_queue);

        if (params->worker_done) {
            break;
        }
        if (!the_queue->last_valid) {
            continue;
        }
        receipt_timestamp = the_queue->last_receipt;

        clock_gettime(CLOCK_MONOTONIC, &start_timestamp);

        // process the request by busywaiting for the requested length
        get_elapsed_busywait(req.request_length.tv_sec, req.request_length.tv_nsec);

        // send the response back to the client
        resp.request_id = req.request_id;
        resp.response = 0;
        resp.ack = 0;
        send(params->conn_socket, &resp, sizeof(struct response), 0);

        clock_gettime(CLOCK_MONOTONIC, &completion_timestamp);

        printf("R%lu:%.6f,%.6f,%.6f,%.6f,%.6f\n",
               (unsigned long)req.request_id,
               TSPEC_TO_DOUBLE(req.sent_timestamp),
               TSPEC_TO_DOUBLE(req.request_length),
               TSPEC_TO_DOUBLE(receipt_timestamp),
               TSPEC_TO_DOUBLE(start_timestamp),
               TSPEC_TO_DOUBLE(completion_timestamp));

        dump_queue_status(the_queue);
    }

    return NULL;
}

/* main function to handle the connectino with the client. this function takes in 
   input conn_socket and returns only when the connection with the client is 
   interrupted. 
*/
void handle_connection(int conn_socket) {
    struct request * req;
    struct queue * the_queue;
    size_t in_bytes;
    struct WORKER_PASS * worker_params;
    pthread_t worker_thread;
    struct timespec receipt_timestamp;

    /*
    the connection wiht the client is alive here. initialize the shared queue.

    IMPLEMENT HERE ANY QUEUE INITIALIZATION LOGIC 

    queue is ready to go here. lets start with the worker thread. 

    IMPLEMENT HERE THE LOGIC TO START THE WORKER THREAD. 

    we are ready to proceed with the rest of the request handling logic. 

    REUSE LOGIC FROM HW1 TO HANDLE THE PACKETS.
    */
    the_queue = (struct queue *)malloc(sizeof(struct queue));
    memset(the_queue, 0, sizeof(struct queue));

    worker_params = (struct WORKER_PASS *)malloc(sizeof(struct WORKER_PASS));
    worker_params->conn_socket = conn_socket;
    worker_params->the_queue = the_queue;
    worker_params->worker_done = 0;

    if (pthread_create(&worker_thread, NULL, worker_main, (void *)worker_params) != 0) {
        ERROR_INFO();
        perror("Unable to create worker thread");
        free(worker_params);
        free(the_queue);
        close(conn_socket);
        return;
    }

    req = (struct request *)malloc(sizeof(struct request));

    do {
        // in_bytes = recv(conn_socket, ... /* implement me */);
        in_bytes = recv(conn_socket, req, sizeof(struct request), 0);
        /* SAMPLE receipt_timestamp HERE

        don't just return if_bytes is 0 or -1. Instead
        skip the repsonse and break out of the loop
        in an orderly fashion so that we can de-allocate
        the req and resp variables,a nd shutdown the socket
        */
        clock_gettime(CLOCK_MONOTONIC, &receipt_timestamp);

        // in_bytes is unsigned, so treat an error (-1) the same as 0
        if ((ssize_t)in_bytes <= 0) {
            in_bytes = 0;
        }

        if (in_bytes > 0) {
            the_queue->next_receipt = receipt_timestamp;
            add_to_queue(*req, the_queue);
        }
    } while (in_bytes > 0);
    /* 
    perform dellocation and outro here 

    ask the worker thread to temrinate 

    assert termination flag for the worker thread 

    make sure to wake-up any thread left stuck waiting for items in the queue
    */
    free(req);
    worker_params->worker_done = 1;
    sem_post(queue_notify);

    /*
    wait for termination of the worker thread

    add here logic to wait for termination of worker

    free up data structures and shutdown connection with client
    */
    pthread_join(worker_thread, NULL);

    free(worker_params);
    free(the_queue);

    shutdown(conn_socket, SHUT_RDWR);
    close(conn_socket);
}

/* template implementation of the main function for the FIFO server. 
   the server must accept in input a command line parameter with the <port number>
   to bind the server to. 
*/

int main (int argc, char ** argv) { 
    int sockfd, retval, accepted, optval;
    in_port_t socket_port; 
    struct sockaddr_in addr, client;
    struct in_addr any_address;
    socklen_t client_len;

    if (argc > 1) {
        socket_port = strtol(argv[1], NULL, 10);
        printf("INFO: setting server port as: %d\n", socket_port);
    } else {
        ERROR_INFO();
        fprintf(stderr, USAGE_STRING, argv[0]);
        return EXIT_FAILURE;
    }

    sockfd = socket(AF_INET, SOCK_STREAM, 0); 

    if (sockfd < 0) { 
        ERROR_INFO();
        perror("Unable to create socket");
        return EXIT_FAILURE; 
    }

    optval = 1;
    setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, (void *)&optval, sizeof(optval));

    any_address.s_addr = htonl(INADDR_ANY);

    addr.sin_family = AF_INET;
    addr.sin_port = htons(socket_port);
    addr.sin_addr = any_address;

    retval = bind(sockfd, (struct sockaddr *)&addr, sizeof(struct sockaddr_in));

    if (retval < 0) {
        ERROR_INFO();
        perror("Unable to bind socket");
        return EXIT_FAILURE;
    }

    retval = listen(sockfd, BACKLOG_COUNT);

    if (retval < 0) {
        ERROR_INFO();
        perror("Unable to listen on socket");
        return EXIT_FAILURE;
    }
    printf("INFO: Using timelib version %s\n", TIMELIB_VER);
    printf("INFO: Waiting for incoming connection...\n");
    client_len = sizeof(struct sockaddr_in);
    accepted = accept(sockfd, (struct sockaddr *)&client, &client_len);

    if (accepted == -1) {
        ERROR_INFO();
        perror("Unable to accept connections");
        return EXIT_FAILURE;
    }

    queue_mutex = (sem_t *)malloc(sizeof(sem_t));
	queue_notify = (sem_t *)malloc(sizeof(sem_t));
	retval = sem_init(queue_mutex, 0, 1);
	if (retval < 0) {
		ERROR_INFO();
		perror("Unable to initialize queue mutex");
		return EXIT_FAILURE;
	}
	retval = sem_init(queue_notify, 0, 0);
	if (retval < 0) {
		ERROR_INFO();
		perror("Unable to initialize queue notify");
		return EXIT_FAILURE;
	}
	handle_connection(accepted);

	free(queue_mutex);
	free(queue_notify);

	close(sockfd);
	return EXIT_SUCCESS;
}



