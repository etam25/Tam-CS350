#define _GNU_SOURCE
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <sched.h>
#include <signal.h>
#include <pthread.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/* needed for wait(...) */
#include <sys/types.h>
#include <sys/wait.h>

/* needed for semaphores */
#include <semaphore.h>

/*
inlcude struct definitions and other libraries that needed to be
included by both client and server
*/
#include "common.h"

#define BACKLOG_COUNT 100
#define USAGE_STRING \
    "Missing parameter. Exiting.\n" \
    "Usage: %s -q <queue size> -w <number of threads> <port_number>\n"

/* Values for the ack field of the response */
#define RESP_COMPLETED  0
#define RESP_REJECTED   1

/* Mutex needed to protect the threaded printf. DO NOT TOUCH */
sem_t * printf_mutex;
/* synchronized printf for multi-threaded operation
USE sync_printf(...) INSTEAD OF printf(...) FOR WORKER AND PARENT THREADS
*/
#define sync_printf(...) \
    do {  \
        sem_wait(printf_mutex); \
        printf(__VA_ARGS__); \
        sem_post(printf_mutex); \
    } while (0)

/* START - variables needed to protect the shared queue. DO NOT TOUCH */
sem_t * queue_mutex;
sem_t * queue_notify;
/* END - variables needed to protect the shared queue. DO NOT TOUCH */

struct request_meta {
    struct request request;

    /* ADD REQUIRED FIELDS */
    struct timespec receipt_timestamp; /* when the parent recv'd the request */
};

struct queue {
    /* ADD REQUIRED FIELDS */
    struct request_meta * entries; /* circular buffer of queued requests */
    size_t size;  /* max number of entries (from -q) */
    size_t head;  /* index of the next entry to remove */
    size_t tail;  /* index where the next entry will be inserted */
    size_t count; /* number of entries currently in the queue */
};

struct connection_params {
    /* ADD REQUIRED FIELDS */
    size_t queue_size;   /* from -q */
    size_t worker_count; /* from -w */
};

struct worker_params {
    /* ADD REQUIRED FIELDS */
    int thread_id;            /* 0 .. (workers - 1) */
    int conn_socket;
    struct queue * the_queue; /* same pointer for all the workers */
    pthread_t thread;
    volatile int worker_done; /* termination flag asserted by the parent */
};

void queue_init(struct queue * the_queue, size_t queue_size) {
    /* IMPLEMENT ME */
    memset(the_queue, 0, sizeof(struct queue));
    the_queue->entries = (struct request_meta *)malloc(queue_size * sizeof(struct request_meta));
    the_queue->size = queue_size;
}

/* add a new request <request> to the shared queue <the_queue> */
int add_to_queue(struct request_meta to_add, struct queue * the_queue) {
    int retval = 0;
    /* QUEUE PROTECTION INTRO START --- DO NOT TOUCH */
	sem_wait(queue_mutex);
	/* QUEUE PROTECTION INTRO END --- DO NOT TOUCH */


	/* WRITE YOUR CODE HERE! */
	/* MAKE SURE NOT TO RETURN WITHOUT GOING THROUGH THE OUTRO CODE! */


	/* Make sure that the queue is not full */
	if (the_queue->count >= the_queue->size) {
		/* What to do in case of a full queue */
		/* tell the caller to reject the request */
		retval = 1;

		/* DO NOT RETURN DIRECTLY HERE. The
		 * sem_post(queue_mutex) below MUST happen. */
	} else {
		/* If all good, add the item in the queue */
		/* IMPLEMENT ME !!*/
		the_queue->entries[the_queue->tail] = to_add;
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
struct request_meta get_from_queue(struct queue * the_queue)
{
	struct request_meta retval;
	/* QUEUE PROTECTION INTRO START --- DO NOT TOUCH */
	sem_wait(queue_notify);
	sem_wait(queue_mutex);
	/* QUEUE PROTECTION INTRO END --- DO NOT TOUCH */


	/* WRITE YOUR CODE HERE! */
	/* MAKE SURE NOT TO RETURN WITHOUT GOING THROUGH THE OUTRO CODE! */
	if (the_queue->count == 0) {
		/* only happens when woken up by the parent for termination */
		memset(&retval, 0, sizeof(retval));
	} else {
		retval = the_queue->entries[the_queue->head];
		the_queue->head = (the_queue->head + 1) % the_queue->size;
		the_queue->count--;
	}


	/* QUEUE PROTECTION OUTRO START --- DO NOT TOUCH */
	sem_post(queue_mutex);
	/* QUEUE PROTECTION OUTRO END --- DO NOT TOUCH */
	return retval;
}


/* Dump the status of the queue following the format
 * Q:[R<request ID>,R<request ID>,...]. The caller must hold
 * printf_mutex so that the dump is not interleaved with other output. */
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
		printf("R%lu", (unsigned long)the_queue->entries[idx].request.request_id);
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


/* Main logic of the worker thread */
int worker_main (void * arg)
{
	struct timespec now;
	struct worker_params * params = (struct worker_params *)arg;
	struct request_meta req;
	struct response resp;
	struct timespec start_timestamp, completion_timestamp;


	/* Print the first alive message. */
	clock_gettime(CLOCK_MONOTONIC, &now);
	sync_printf("[#WORKER#] %lf Worker Thread Alive!\n", TSPEC_TO_DOUBLE(now));


	/* Okay, now execute the main logic. */
	while (!params->worker_done) {


		/* IMPLEMENT ME !! Main worker logic. */
		/* blocks until the parent adds something (or asks us to terminate) */
		req = get_from_queue(params->the_queue);

		/* woken up by the parent for termination */
		if (params->worker_done) {
			break;
		}

		clock_gettime(CLOCK_MONOTONIC, &start_timestamp);

		/* process the request by busywaiting for the requested length */
		get_elapsed_busywait(req.request.request_length.tv_sec,
				     req.request.request_length.tv_nsec);

		/* send the response back to the client */
		memset(&resp, 0, sizeof(resp));
		resp.request_id = req.request.request_id;
		resp.response = 0;
		resp.ack = RESP_COMPLETED;
		send(params->conn_socket, &resp, sizeof(struct response), 0);

		clock_gettime(CLOCK_MONOTONIC, &completion_timestamp);

		/* Hold printf_mutex across the report and the queue dump so
		 * the two lines are never split by another thread's output */
		sem_wait(printf_mutex);
		printf("T%d R%lu:%.6f,%.6f,%.6f,%.6f,%.6f\n",
		       params->thread_id,
		       (unsigned long)req.request.request_id,
		       TSPEC_TO_DOUBLE(req.request.sent_timestamp),
		       TSPEC_TO_DOUBLE(req.request.request_length),
		       TSPEC_TO_DOUBLE(req.receipt_timestamp),
		       TSPEC_TO_DOUBLE(start_timestamp),
		       TSPEC_TO_DOUBLE(completion_timestamp));

		dump_queue_status(params->the_queue);
		sem_post(printf_mutex);
	}

	sync_printf("INFO: T%d Done!\n", params->thread_id);

	return EXIT_SUCCESS;
}


/* pthread entry point: pthread_create(...) needs a void * (*)(void *) */
void * worker_thread_entry(void * arg)
{
	worker_main(arg);
	return NULL;
}


/* This function will control all the workers (start or stop them).
 * Feel free to change the arguments of the function as you wish. */
int control_workers(int start_stop_cmd, size_t worker_count, struct worker_params * common_params)
{
	/* IMPLEMENT ME !! */
	/* common_params is an array of worker_count entries, one per worker */
	size_t i;
	int retval = EXIT_SUCCESS;

	if (start_stop_cmd == 0) { // Starting all the workers
		/* IMPLEMENT ME !! */
		for (i = 0; i < worker_count; i++) {
			common_params[i].thread_id = (int)i;
			common_params[i].worker_done = 0;
			if (pthread_create(&common_params[i].thread, NULL,
					   worker_thread_entry, &common_params[i]) != 0) {
				ERROR_INFO();
				perror("Unable to create worker thread");
				retval = EXIT_FAILURE;
				break;
			}
		}
	} else { // Stopping all the workers
		/* IMPLEMENT ME !! */
		/* Ask all the worker theads to terminate */
		for (i = 0; i < worker_count; i++) {
			common_params[i].worker_done = 1;
		}

		/* Wake-up every thread left stuck waiting for items in the queue */
		for (i = 0; i < worker_count; i++) {
			sem_post(queue_notify);
		}

		/* Wait for orderly termination of all the worker threads */
		for (i = 0; i < worker_count; i++) {
			pthread_join(common_params[i].thread, NULL);
		}
	}

	/* IMPLEMENT ME !! */
	return retval;
}


/* Main function to handle connection with the client. This function
 * takes in input conn_socket and returns only when the connection
 * with the client is interrupted. */
void handle_connection(int conn_socket, struct connection_params conn_params)
{
	struct request_meta * req;
	struct queue * the_queue;
	size_t in_bytes;
	struct worker_params * worker_params;
	struct response resp;
	struct timespec reject_timestamp;
	size_t i;


	/* IMPLEMENT ME!! Start and initialize all the
	 * worker threads ! */
	the_queue = (struct queue *)malloc(sizeof(struct queue));
	queue_init(the_queue, conn_params.queue_size);

	worker_params = (struct worker_params *)malloc(conn_params.worker_count * sizeof(struct worker_params));
	for (i = 0; i < conn_params.worker_count; i++) {
		worker_params[i].conn_socket = conn_socket;
		worker_params[i].the_queue = the_queue;
	}

	if (control_workers(0, conn_params.worker_count, worker_params) != EXIT_SUCCESS) {
		free(worker_params);
		free(the_queue->entries);
		free(the_queue);
		close(conn_socket);
		return;
	}


	/* We are ready to proceed with the rest of the request
	 * handling logic. */


	req = (struct request_meta *)malloc(sizeof(struct request_meta));


	do {
		/* IMPLEMENT ME: Receive next request from socket. */
		in_bytes = recv(conn_socket, &req->request, sizeof(struct request), 0);
		clock_gettime(CLOCK_MONOTONIC, &req->receipt_timestamp);


		/* Don't just return if in_bytes is 0 or -1. Instead
		 * skip the response and break out of the loop in an
		 * orderly fashion so that we can de-allocate the req
		 * and resp varaibles, and shutdown the socket. */
		/* in_bytes is unsigned, so treat an error (-1) the same as 0 */
		if ((ssize_t)in_bytes <= 0) {
			in_bytes = 0;
			break;
		}


		/* IMPLEMENT ME: Attempt to enqueue or reject request! */
		if (add_to_queue(*req, the_queue) != 0) {
			/* Queue full: reject the request immediately */
			clock_gettime(CLOCK_MONOTONIC, &reject_timestamp);

			memset(&resp, 0, sizeof(resp));
			resp.request_id = req->request.request_id;
			resp.response = 0;
			resp.ack = RESP_REJECTED;
			send(conn_socket, &resp, sizeof(struct response), 0);

			sem_wait(printf_mutex);
			printf("X%lu:%.6f,%.6f,%.6f\n",
			       (unsigned long)req->request.request_id,
			       TSPEC_TO_DOUBLE(req->request.sent_timestamp),
			       TSPEC_TO_DOUBLE(req->request.request_length),
			       TSPEC_TO_DOUBLE(reject_timestamp));
			dump_queue_status(the_queue);
			sem_post(printf_mutex);
		}
	} while (in_bytes > 0);


	/* IMPLEMENT ME!! Gracefully terminate all the worker threads ! */
	control_workers(1, conn_params.worker_count, worker_params);
	free(worker_params);
	free(the_queue->entries);


	free(the_queue);


	free(req);
	shutdown(conn_socket, SHUT_RDWR);
	close(conn_socket);
	sync_printf("INFO: Client disconnected.\n");
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
	long worker_count = -1;


	struct connection_params conn_params;


	/* Parse all the required and optional command line arguments */
	/* IMPLEMENT ME!! */
	/* PARSE THE COMMANDS LINE: */
	while ((opt = getopt(argc, argv, "q:w:")) != -1) {
		switch (opt) {
		case 'q':
			queue_size = strtol(optarg, NULL, 10);
			break;
		case 'w':
			worker_count = strtol(optarg, NULL, 10);
			break;
		default:
			fprintf(stderr, USAGE_STRING, argv[0]);
			return EXIT_FAILURE;
		}
	}

	/* 1. Detect the -q parameter and set aside the queue size in conn_params */
	if (queue_size <= 0) {
		ERROR_INFO();
		fprintf(stderr, USAGE_STRING, argv[0]);
		return EXIT_FAILURE;
	}
	conn_params.queue_size = (size_t)queue_size;
	printf("INFO: setting queue size as: %ld\n", queue_size);

	/* 2. Detect the -w parameter and set aside the number of threads to launch */
	if (worker_count <= 0) {
		ERROR_INFO();
		fprintf(stderr, USAGE_STRING, argv[0]);
		return EXIT_FAILURE;
	}
	conn_params.worker_count = (size_t)worker_count;
	printf("INFO: setting number of workers as: %ld\n", worker_count);

	/* 3. Detect the port number to bind the server socket to (see HW1 and HW2) */
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


	/* Initilize threaded printf mutex */
	printf_mutex = (sem_t *)malloc(sizeof(sem_t));
	retval = sem_init(printf_mutex, 0, 1);
	if (retval < 0) {
		ERROR_INFO();
		perror("Unable to initialize printf mutex");
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
	/* DONE - Initialize queue protection variables */


	/* Ready to handle the new connection with the client. */
	handle_connection(accepted, conn_params);


	free(queue_mutex);
	free(queue_notify);
	free(printf_mutex);


	close(sockfd);
	return EXIT_SUCCESS;
}

/* End of server_multi implementation version 6z8m7p8l6u4p7l */
