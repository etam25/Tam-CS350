/*******************************************************************************
* Dual-Threaded FIFO Server Implementation w/ Queue Limit
*
* Description:
*     A server implementation designed to process client requests in First In,
*     First Out (FIFO) order. The server binds to the specified port number
*     provided as a parameter upon launch. It launches a secondary thread to
*     process incoming requests and allows to specify a maximum queue size.
*
* Usage:
*     <build directory>/server -q <queue_size> [-k <max_rejects>] <port_number>
*
* Parameters:
*     port_number - The port number to bind the server to.
*     queue_size  - The maximum number of queued requests
*     max_rejects - (optional) close the connection once more than this many
*                   requests have been rejected
*
* Author:
*     Renato Mancuso
*
* Affiliation:
*     Boston University
*
* Creation Date:
*     September 29, 2023
*
* Last Update:
*     September 24, 2026
*
* Notes:
*     Ensure to have proper permissions and available port before running the
*     server. The server relies on a FIFO mechanism to handle requests, thus
*     guaranteeing the order of processing. If the queue is full at the time a
*     new request is received, the request is rejected with a negative ack.
*
*******************************************************************************/

#define _GNU_SOURCE
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <sched.h>
#include <signal.h>

/* Needed for wait(...) */
#include <sys/types.h>
#include <sys/wait.h>

/* Needed for semaphores */
#include <semaphore.h>

#include <pthread.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/* Include struct definitions and other libraries that need to be
 * included by both client and server */
#include "common.h"

#define BACKLOG_COUNT 100
#define USAGE_STRING				\
	"Missing parameter. Exiting.\n"		\
	"Usage: %s -q <queue size> [-k <max rejects>] <port_number>\n"

/* Values for the ack field of the response */
#ifndef RESP_COMPLETED
#define RESP_COMPLETED  0
#endif
#ifndef RESP_REJECTED
#define RESP_REJECTED   1
#endif
#define RESP_OUT_OF_ORDER 2

/* START - Variables needed to protect the shared queue. DO NOT TOUCH */
sem_t * queue_mutex;
sem_t * queue_notify;
/* END - Variables needed to protect the shared queue. DO NOT TOUCH */

/* Serializes output so that a report line and the queue dump that
 * follows it are never interleaved between the parent and the worker */
pthread_mutex_t print_mutex = PTHREAD_MUTEX_INITIALIZER;

/* Max number of requests that can be queued */
#define QUEUE_SIZE 500

/* one slot in the queue: the request plus the time the parent recv'd it */
struct queue_entry {
	struct request req;
	struct timespec receipt_timestamp;
};

/* circular buffer of requests shared between parent and worker */
struct queue {
	struct queue_entry * entries;
	size_t size;   /* max number of entries (from -q) */
	size_t head;   /* index of the next entry to remove */
	size_t tail;   /* index where the next entry will be inserted */
	size_t count;  /* number of entries currently in the queue */

	/* set by the parent right before add_to_queue(...) */
	struct timespec next_receipt;
	/* set by get_from_queue(...) for the entry it just returned */
	struct timespec last_receipt;
	int last_valid; /* 1 if the last get_from_queue(...) returned a real request */
};

struct worker_params {
	int conn_socket;
	struct queue * the_queue;
	volatile int worker_done; /* termination flag asserted by the parent */
};

/* Helper function to find a request with a given ID. Modify as needed. */
static int __attribute__((unused))
find_request(const struct request *requests, size_t count, uint64_t id)
{
	size_t i;

	if (count == 0 || requests == NULL)
		return -1;

	for (i = count - 1; i != (size_t)-1; --i) {
		if (requests[i].request_id == id)
			return (int)i;
	}

	return -1;
}

/* Helper function to perform queue initialization */
void queue_init(struct queue * the_queue, size_t queue_size)
{
	memset(the_queue, 0, sizeof(struct queue));
	the_queue->entries = (struct queue_entry *)malloc(queue_size * sizeof(struct queue_entry));
	the_queue->size = queue_size;
}

/* Add a new request <request> to the shared queue <the_queue> */
int add_to_queue(struct request to_add, struct queue * the_queue)
{
	int retval = 0;
	/* QUEUE PROTECTION INTRO START --- DO NOT TOUCH */
	sem_wait(queue_mutex);
	/* QUEUE PROTECTION INTRO END --- DO NOT TOUCH */

	/* WRITE YOUR CODE HERE! */
	/* MAKE SURE NOT TO RETURN WITHOUT GOING THROUGH THE OUTRO CODE! */

	/* Make sure that the queue is not full */
	if (the_queue->count >= the_queue->size) {
		/* queue is full: tell the caller to reject the request */
		retval = 1;
	} else {
		/* If all good, add the item in the queue */
		the_queue->entries[the_queue->tail].req = to_add;
		the_queue->entries[the_queue->tail].receipt_timestamp = the_queue->next_receipt;
		the_queue->tail = (the_queue->tail + 1) % the_queue->size;
		the_queue->count++;

		/* QUEUE SIGNALING FOR CONSUMER --- DO NOT TOUCH */
		sem_post(queue_notify);
	}

	/* QUEUE PROTECTION OUTRO START --- DO NOT TOUCH */
	sem_post(queue_mutex);
	/* QUEUE PROTECTION OUTRO END --- DO NOT TOUCH */
	return retval;
}

/* Add a new request <request> to the shared queue <the_queue> */
struct request get_from_queue(struct queue * the_queue)
{
	struct request retval;
	/* QUEUE PROTECTION INTRO START --- DO NOT TOUCH */
	sem_wait(queue_notify);
	sem_wait(queue_mutex);
	/* QUEUE PROTECTION INTRO END --- DO NOT TOUCH */

	/* WRITE YOUR CODE HERE! */
	/* MAKE SURE NOT TO RETURN WITHOUT GOING THROUGH THE OUTRO CODE! */
	if (the_queue->count == 0) {
		/* only happens when woken up for termination */
		memset(&retval, 0, sizeof(retval));
		the_queue->last_valid = 0;
	} else {
		retval = the_queue->entries[the_queue->head].req;
		the_queue->last_receipt = the_queue->entries[the_queue->head].receipt_timestamp;
		the_queue->last_valid = 1;
		the_queue->head = (the_queue->head + 1) % the_queue->size;
		the_queue->count--;
	}

	/* QUEUE PROTECTION OUTRO START --- DO NOT TOUCH */
	sem_post(queue_mutex);
	/* QUEUE PROTECTION OUTRO END --- DO NOT TOUCH */
	return retval;
}

/* Implement this method to correctly dump the status of the queue
 * following the format Q:[R<request ID>,R<request ID>,...] */
void dump_queue_status(struct queue * the_queue)
{
	size_t i;
	/* QUEUE PROTECTION INTRO START --- DO NOT TOUCH */
	sem_wait(queue_mutex);
	/* QUEUE PROTECTION INTRO END --- DO NOT TOUCH */

	/* WRITE YOUR CODE HERE! */
	/* MAKE SURE NOT TO RETURN WITHOUT GOING THROUGH THE OUTRO CODE! */
	printf("Q:[");
	for (i = 0; i < the_queue->count; i++) {
		size_t idx = (the_queue->head + i) % the_queue->size;
		printf("R%lu", (unsigned long)the_queue->entries[idx].req.request_id);
		if (i < the_queue->count - 1) {
			printf(",");
		}
	}
	printf("]\n");
	fflush(stdout);

	/* QUEUE PROTECTION OUTRO START --- DO NOT TOUCH */
	sem_post(queue_mutex);
	/* QUEUE PROTECTION OUTRO END --- DO NOT TOUCH */
}

/* Send an immediate (non-processed) response to the client and print
 * the X report followed by the queue status */
static void reject_request(int conn_socket, struct request * req,
			   struct queue * the_queue, uint8_t ack)
{
	struct response resp;
	struct timespec reject_timestamp;

	clock_gettime(CLOCK_MONOTONIC, &reject_timestamp);

	memset(&resp, 0, sizeof(resp));
	resp.request_id = req->request_id;
	resp.response = 0;
	resp.ack = ack;
	send(conn_socket, &resp, sizeof(struct response), 0);

	pthread_mutex_lock(&print_mutex);
	printf("X%lu:%.6f,%.6f,%.6f\n",
	       (unsigned long)req->request_id,
	       TSPEC_TO_DOUBLE(req->sent_timestamp),
	       TSPEC_TO_DOUBLE(req->request_length),
	       TSPEC_TO_DOUBLE(reject_timestamp));
	dump_queue_status(the_queue);
	pthread_mutex_unlock(&print_mutex);
}

/* Main logic of the worker thread */
void * worker_main(void * arg)
{
	struct worker_params * params = (struct worker_params *)arg;
	struct queue * the_queue = params->the_queue;
	struct request req;
	struct response resp;
	struct timespec receipt_timestamp, start_timestamp, completion_timestamp;

	while (!params->worker_done) {
		/* blocks until the parent adds something (or asks us to terminate) */
		req = get_from_queue(the_queue);

		if (params->worker_done) {
			break;
		}
		if (!the_queue->last_valid) {
			continue;
		}
		receipt_timestamp = the_queue->last_receipt;

		clock_gettime(CLOCK_MONOTONIC, &start_timestamp);

		/* process the request by busywaiting for the requested length */
		get_elapsed_busywait(req.request_length.tv_sec, req.request_length.tv_nsec);

		/* send the response back to the client */
		memset(&resp, 0, sizeof(resp));
		resp.request_id = req.request_id;
		resp.response = 0;
		resp.ack = RESP_COMPLETED;
		send(params->conn_socket, &resp, sizeof(struct response), 0);

		clock_gettime(CLOCK_MONOTONIC, &completion_timestamp);

		pthread_mutex_lock(&print_mutex);
		printf("R%lu:%.6f,%.6f,%.6f,%.6f,%.6f\n",
		       (unsigned long)req.request_id,
		       TSPEC_TO_DOUBLE(req.sent_timestamp),
		       TSPEC_TO_DOUBLE(req.request_length),
		       TSPEC_TO_DOUBLE(receipt_timestamp),
		       TSPEC_TO_DOUBLE(start_timestamp),
		       TSPEC_TO_DOUBLE(completion_timestamp));
		dump_queue_status(the_queue);
		pthread_mutex_unlock(&print_mutex);
	}

	return NULL;
}

/* Main function to handle connection with the client. This function
 * takes in input conn_socket and returns only when the connection
 * with the client is interrupted. */
void handle_connection(int conn_socket, size_t queue_size, long max_rejects)
{
	struct request * req;
	struct queue * the_queue;
	size_t in_bytes;
	struct worker_params * worker_params;
	pthread_t worker_thread;
	struct timespec receipt_timestamp;
	long reject_count = 0;
	int seen_any = 0;
	uint64_t last_id = 0;

	/* The connection with the client is alive here. Let's
	 * initialize the shared queue. */
	the_queue = (struct queue *)malloc(sizeof(struct queue));
	queue_init(the_queue, queue_size);

	/* Queue ready to go here. Let's start the worker thread. */
	worker_params = (struct worker_params *)malloc(sizeof(struct worker_params));
	worker_params->conn_socket = conn_socket;
	worker_params->the_queue = the_queue;
	worker_params->worker_done = 0;

	if (pthread_create(&worker_thread, NULL, worker_main, (void *)worker_params) != 0) {
		ERROR_INFO();
		perror("Unable to create worker thread");
		free(worker_params);
		free(the_queue->entries);
		free(the_queue);
		close(conn_socket);
		return;
	}

	/* We are ready to proceed with the rest of the request
	 * handling logic. */
	req = (struct request *)malloc(sizeof(struct request));

	do {
		in_bytes = recv(conn_socket, req, sizeof(struct request), 0);
		clock_gettime(CLOCK_MONOTONIC, &receipt_timestamp);

		/* in_bytes is unsigned, so treat an error (-1) the same as 0 */
		if ((ssize_t)in_bytes <= 0) {
			in_bytes = 0;
		}

		if (in_bytes > 0) {
			/* Out of order (or duplicate) request ID: reply right
			 * away with ack = 2 and do not enqueue it */
			if (seen_any && req->request_id <= last_id) {
				reject_request(conn_socket, req, the_queue, RESP_OUT_OF_ORDER);
				continue;
			}
			seen_any = 1;
			last_id = req->request_id;

			the_queue->next_receipt = receipt_timestamp;
			if (add_to_queue(*req, the_queue) != 0) {
				/* Queue full: reject the request immediately */
				reject_request(conn_socket, req, the_queue, RESP_REJECTED);

				/* Close the connection after too many rejections */
				reject_count++;
				if (max_rejects >= 0 && reject_count > max_rejects) {
					break;
				}
			}
		}
	} while (in_bytes > 0);

	/* PERFORM ORDERLY DEALLOCATION AND OUTRO HERE */
	free(req);

	/* Ask the worker thead to terminate */
	worker_params->worker_done = 1;

	/* Make sure to wake-up any thread left stuck waiting for items in the queue. DO NOT TOUCH */
	sem_post(queue_notify);

	/* Wait for orderly termination of the worker thread */
	pthread_join(worker_thread, NULL);

	/* FREE UP DATA STRUCTURES AND SHUTDOWN CONNECTION WITH CLIENT */
	free(worker_params);
	free(the_queue->entries);
	free(the_queue);

	shutdown(conn_socket, SHUT_RDWR);
	close(conn_socket);
}


/* Template implementation of the main function for the FIFO
 * server. The server must accept in input a command line parameter
 * with the <port number> to bind the server to. */
int main (int argc, char ** argv) {
	int sockfd, retval, accepted, optval, opt;
	in_port_t socket_port;
	struct sockaddr_in addr, client;
	struct in_addr any_address;
	socklen_t client_len;
	long queue_size = -1;
	long max_rejects = -1;

	/* Parse all the command line arguments */
	/* 1. Detect the -q parameter (and optional -k) */
	while ((opt = getopt(argc, argv, "q:k:")) != -1) {
		switch (opt) {
		case 'q':
			queue_size = strtol(optarg, NULL, 10);
			break;
		case 'k':
			max_rejects = strtol(optarg, NULL, 10);
			break;
		default:
			fprintf(stderr, USAGE_STRING, argv[0]);
			return EXIT_FAILURE;
		}
	}

	if (queue_size <= 0) {
		ERROR_INFO();
		fprintf(stderr, USAGE_STRING, argv[0]);
		return EXIT_FAILURE;
	}
	printf("INFO: setting queue size as: %ld\n", queue_size);

	/* 2. Detect the port number to bind the server socket to (see HW1 and HW2) */
	if (optind < argc) {
		socket_port = strtol(argv[optind], NULL, 10);
		printf("INFO: setting server port as: %d\n", socket_port);
	} else {
		ERROR_INFO();
		fprintf(stderr, USAGE_STRING, argv[0]);
		return EXIT_FAILURE;
	}

	/* Don't die if the client goes away while we are still sending */
	signal(SIGPIPE, SIG_IGN);

	/* Now onward to create the right type of socket */
	sockfd = socket(AF_INET, SOCK_STREAM, 0);

	if (sockfd < 0) {
		ERROR_INFO();
		perror("Unable to create socket");
		return EXIT_FAILURE;
	}

	/* Before moving forward, set socket to reuse address */
	optval = 1;
	setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, (void *)&optval, sizeof(optval));

	/* Convert INADDR_ANY into network byte order */
	any_address.s_addr = htonl(INADDR_ANY);

	/* Time to bind the socket to the right port  */
	addr.sin_family = AF_INET;
	addr.sin_port = htons(socket_port);
	addr.sin_addr = any_address;

	/* Attempt to bind the socket with the given parameters */
	retval = bind(sockfd, (struct sockaddr *)&addr, sizeof(struct sockaddr_in));

	if (retval < 0) {
		ERROR_INFO();
		perror("Unable to bind socket");
		return EXIT_FAILURE;
	}

	/* Let us now proceed to set the server to listen on the selected port */
	retval = listen(sockfd, BACKLOG_COUNT);

	if (retval < 0) {
		ERROR_INFO();
		perror("Unable to listen on socket");
		return EXIT_FAILURE;
	}

	/* Ready to accept connections! */
	printf("INFO: Using timelib version %s\n", TIMELIB_VER);
	printf("INFO: Waiting for incoming connection...\n");
	client_len = sizeof(struct sockaddr_in);
	accepted = accept(sockfd, (struct sockaddr *)&client, &client_len);

	if (accepted == -1) {
		ERROR_INFO();
		perror("Unable to accept connections");
		return EXIT_FAILURE;
	}

	/* Initialize queue protection variables. DO NOT TOUCH. */
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
	/* DONE - Initialize queue protection variables. DO NOT TOUCH */

	/* Ready to handle the new connection with the client. */
	handle_connection(accepted, (size_t)queue_size, max_rejects);

	free(queue_mutex);
	free(queue_notify);

	close(sockfd);
	return EXIT_SUCCESS;

}

/* End of server_lim implementation version 5y2g2k4h7v6r6k */
