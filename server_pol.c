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

/* Needed for wait(...) */
#include <sys/types.h>
#include <sys/wait.h>

/* Needed for semaphores */
#include <semaphore.h>

/* Include struct definitions and other libraries that need to be
 * included by both client and server */
#include "common.h"

#define BACKLOG_COUNT 100
#define USAGE_STRING				\
	"Missing parameter. Exiting.\n"		\
	"Usage: %s -q <queue size> "		\
	"-w <workers> "				\
	"-p <policy: FIFO | SJN | HSN> "	\
	"<port_number>\n"


// define values for the ack field of the response
#define RESP_COMPLETED 0
#define RESP_REJECTED 1

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


/* START - Variables needed to protect the shared queue. DO NOT TOUCH */
sem_t * queue_mutex;
sem_t * queue_notify;
/* END - Variables needed to protect the shared queue. DO NOT TOUCH */

struct request_meta {
	struct request request;
	struct timespec receipt_timestamp;
	struct timespec start_timestamp;
	struct timespec completion_timestamp;
};

enum queue_policy {
	QUEUE_FIFO,
	QUEUE_SJN
};

struct queue {
	size_t wr_pos;
	size_t rd_pos;
	size_t max_size;
	size_t available;
	struct request_meta * requests;
};

struct connection_params {
	size_t queue_size;
	size_t workers;
	enum queue_policy policy;
};

struct worker_params {
	int conn_socket;
	int worker_done;
	struct queue * the_queue;
	int worker_id;
};

enum worker_command {
	WORKERS_START,
	WORKERS_STOP
};

void queue_init(struct queue * the_queue, size_t queue_size)
{
	the_queue->rd_pos = 0;
	the_queue->wr_pos = 0;
	the_queue->max_size = queue_size;
	the_queue->requests = (struct request_meta *)malloc(sizeof(struct request_meta)
						     * the_queue->max_size);
	the_queue->available = queue_size;

}

/* Add a new request <request> to the shared queue <the_queue> */
int add_to_queue(struct request_meta to_add, struct queue * the_queue, enum queue_policy policy) {
	int retval = 0;
	/* QUEUE PROTECTION INTRO START --- DO NOT TOUCH */
	sem_wait(queue_mutex);
	/* QUEUE PROTECTION INTRO END --- DO NOT TOUCH */

	/* WRITE YOUR CODE HERE! */
	/* MAKE SURE NOT TO RETURN WITHOUT GOING THROUGH THE OUTRO CODE! */

	/* Make sure that the queue is not full */
	if (the_queue->available == 0) {
		retval = 1;
	} else { // fifo or sjn
        if(policy == QUEUE_FIFO) { // fifo - add request at the write position
            the_queue->requests[the_queue->wr_pos] = to_add;
            the_queue->wr_pos = (the_queue->wr_pos + 1) % the_queue->max_size;
        } else if (policy == QUEUE_SJN) { // sjn - insert the request based on its execution time
            size_t count = the_queue->max_size - the_queue->available;
            size_t insert_pos = the_queue->wr_pos;

            for (size_t j = 0; j < count; j++) {
                size_t i = (the_queue->rd_pos + j) % the_queue->max_size;

                if (to_add.request.request_length.tv_sec <
                        the_queue->requests[i].request.request_length.tv_sec ||
                    (to_add.request.request_length.tv_sec ==
                        the_queue->requests[i].request.request_length.tv_sec &&
                    to_add.request.request_length.tv_nsec <
                        the_queue->requests[i].request.request_length.tv_nsec)) {

                    insert_pos = i;
                    break;
                }
            }

            for (size_t i = the_queue->wr_pos;
                i != insert_pos;
                i = (i - 1 + the_queue->max_size) % the_queue->max_size) {

                the_queue->requests[i] =
                    the_queue->requests[
                        (i - 1 + the_queue->max_size) % the_queue->max_size
                    ];
            }

            the_queue->requests[insert_pos] = to_add;
            the_queue->wr_pos = (the_queue->wr_pos + 1) % the_queue->max_size;
        }

        the_queue->available--;


	  /* If all good, add the item in the queue */
	  /* IMPLEMENT ME !!*/

	  /* OPTION 1: After a correct ADD operation, sort the
	   * entire queue. */

	  /* OPTION 2: Find where to place the request in the
	   * queue and shift all the other entries by one
	   * position to the right. */

	  /* OPTION 3: Do nothing different from FIFO case,
	   * and deal with the SJN policy at dequeue time.*/

	  /* QUEUE SIGNALING FOR CONSUMER --- DO NOT TOUCH */
	  /* Only signal after a successful insert, so a rejected
	   * request never wakes a worker on an empty queue */
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

    sem_wait(queue_notify);
    sem_wait(queue_mutex);

    if (the_queue->available == the_queue->max_size) {
        // Queue is empty (possibly shutdown notification)
        memset(&retval, 0, sizeof(retval));
    } else {
        retval = the_queue->requests[the_queue->rd_pos];

        the_queue->rd_pos =
            (the_queue->rd_pos + 1) % the_queue->max_size;

        the_queue->available++;
    }

    sem_post(queue_mutex);
    return retval;
}

void dump_queue_status(struct queue * the_queue)
{
	size_t i, j;
	/* QUEUE PROTECTION INTRO START --- DO NOT TOUCH */
	sem_wait(queue_mutex);
	/* QUEUE PROTECTION INTRO END --- DO NOT TOUCH */

	/* WRITE YOUR CODE HERE! */
	/* MAKE SURE NOT TO RETURN WITHOUT GOING THROUGH THE OUTRO CODE! */
	printf("Q:[");

	for (i = the_queue->rd_pos, j = 0; j < the_queue->max_size - the_queue->available;
	     i = (i + 1) % the_queue->max_size, ++j)
	{
		printf("R%lu%s", (unsigned long)the_queue->requests[i].request.request_id,
		       ((j+1 != the_queue->max_size - the_queue->available)?",":""));
	}

	printf("]\n");
    fflush(stdout);
	/* QUEUE PROTECTION OUTRO START --- DO NOT TOUCH */
	sem_post(queue_mutex);
	/* QUEUE PROTECTION OUTRO END --- DO NOT TOUCH */
}

/* Main logic of the worker thread */
void *worker_main (void * arg)
{
	struct timespec now;
	struct worker_params * params = (struct worker_params *)arg;

	/* Print the first alive message. */
	clock_gettime(CLOCK_MONOTONIC, &now);
	sync_printf("[#WORKER#] %lf Worker Thread Alive!\n", TSPEC_TO_DOUBLE(now));

	/* Okay, now execute the main logic. */
	while (!params->worker_done) {

		struct request_meta req;
		struct response resp;
		req = get_from_queue(params->the_queue);

		if (params->worker_done) {
			break;
		}

		clock_gettime(CLOCK_MONOTONIC, &req.start_timestamp);
		get_elapsed_busywait(req.request.request_length.tv_sec,
				     req.request.request_length.tv_nsec);
		clock_gettime(CLOCK_MONOTONIC, &req.completion_timestamp);

		/* Now provide a response! */
		memset(&resp, 0, sizeof(resp));
		resp.request_id = req.request.request_id;
		resp.ack = RESP_COMPLETED;
		send(params->conn_socket, &resp, sizeof(struct response), 0);

		/* Hold printf_mutex across the report and the queue dump so
		 * the two lines are never split by another thread's output */
		sem_wait(printf_mutex);
		printf("T%d R%lu:%lf,%lf,%lf,%lf,%lf\n",
		       params->worker_id,
		       (unsigned long)req.request.request_id,
		       TSPEC_TO_DOUBLE(req.request.sent_timestamp),
		       TSPEC_TO_DOUBLE(req.request.request_length),
		       TSPEC_TO_DOUBLE(req.receipt_timestamp),
		       TSPEC_TO_DOUBLE(req.start_timestamp),
		       TSPEC_TO_DOUBLE(req.completion_timestamp)
			);

		dump_queue_status(params->the_queue);
		sem_post(printf_mutex);
	}

	return NULL;
}

/* This function will start/stop all the worker threads */
int control_workers(enum worker_command cmd, size_t worker_count,
		    struct worker_params * common_params)
{
	/* Anything we allocate should we kept as static for easy
	 * deallocation when the STOP command is issued */
	static pthread_t * worker_pthreads = NULL;
	static struct worker_params ** worker_params = NULL;
	static int * worker_ids = NULL;

	/* Start all the workers */
	if (cmd == WORKERS_START) {
		size_t i;
		/* Allocate all structs and parameters */
		worker_pthreads = (pthread_t *)malloc(worker_count * sizeof(pthread_t));
		worker_params = (struct worker_params **)
			malloc(worker_count * sizeof(struct worker_params *));
		worker_ids = (int *)malloc(worker_count * sizeof(int));

		if (!worker_pthreads || !worker_params || !worker_ids) {
			ERROR_INFO();
			perror("Unable to allocate arrays for threads.");
			return EXIT_FAILURE;
		}

		/* Allocate and initialize as needed */
		for (i = 0; i < worker_count; ++i) {
			worker_ids[i] = -1;

			worker_params[i] = (struct worker_params *)
				malloc(sizeof(struct worker_params));

			if (!worker_params[i]) {
				ERROR_INFO();
				perror("Unable to allocate memory for thread.");
				return EXIT_FAILURE;
			}

			worker_params[i]->conn_socket = common_params->conn_socket;
			worker_params[i]->the_queue = common_params->the_queue;
			worker_params[i]->worker_done = 0;
			worker_params[i]->worker_id = i;
		}

		/* All the allocations and initialization seem okay,
		 * let's start the threads */
		for (i = 0; i < worker_count; ++i) {
			worker_ids[i] = pthread_create(&worker_pthreads[i], NULL, worker_main, worker_params[i]);

			if (worker_ids[i] < 0) {
				ERROR_INFO();
				perror("Unable to start thread.");
				return EXIT_FAILURE;
			} else {
				sync_printf("INFO: Worker thread %ld (TID = %d) started!\n",
					    i, worker_ids[i]);
			}
		}
	}

	else if (cmd == WORKERS_STOP) {
		size_t i;

		/* Command to stop the threads issues without a start
		 * command? */
		if (!worker_pthreads || !worker_params || !worker_ids) {
			return EXIT_FAILURE;
		}

		/* First, assert all the termination flags */
		for (i = 0; i < worker_count; ++i) {
			if (worker_ids[i] < 0) {
				continue;
			}

			/* Request thread termination */
			worker_params[i]->worker_done = 1;
		}

		/* Next, unblock threads and wait for completion */
		for (i = 0; i < worker_count; ++i) {
			if (worker_ids[i] < 0) {
				continue;
			}

			sem_post(queue_notify);
		}

        for (i = 0; i < worker_count; ++i) {
            pthread_join(worker_pthreads[i],NULL);
            sync_printf("INFO: Worker thread exited.\n");
        }

		/* Finally, do a round of deallocations */
		for (i = 0; i < worker_count; ++i) {
			free(worker_params[i]);
		}

		free(worker_pthreads);
		worker_pthreads = NULL;

		free(worker_params);
		worker_params = NULL;

		free(worker_ids);
		worker_ids = NULL;
	}

	else {
		ERROR_INFO();
		perror("Invalid thread control command.");
		return EXIT_FAILURE;
	}

	return EXIT_SUCCESS;
}

/* Main function to handle connection with the client. This function
 * takes in input conn_socket and returns only when the connection
 * with the client is interrupted. */
void handle_connection(int conn_socket, struct connection_params conn_params)
{
	struct request_meta * req;
	struct queue * the_queue;
	size_t in_bytes;

	/* The connection with the client is alive here. Let's start
	 * the worker thread. */
	struct worker_params common_worker_params;
	int res;

	/* Now handle queue allocation and initialization */
	the_queue = (struct queue *)malloc(sizeof(struct queue));
	queue_init(the_queue, conn_params.queue_size);

	common_worker_params.conn_socket = conn_socket;
	common_worker_params.the_queue = the_queue;
	res = control_workers(WORKERS_START, conn_params.workers, &common_worker_params);

	/* Do not continue if there has been a problem while starting
	 * the workers. */
	if (res != EXIT_SUCCESS) {
		free(the_queue);

		/* Stop any worker that was successfully started */
		control_workers(WORKERS_STOP, conn_params.workers, NULL);
		return;
	}

	/* We are ready to proceed with the rest of the request
	 * handling logic. */

	req = (struct request_meta *)malloc(sizeof(struct request_meta));

	do {
		in_bytes = recv(conn_socket, &req->request, sizeof(struct request), 0);
		clock_gettime(CLOCK_MONOTONIC, &req->receipt_timestamp);

		/* Don't just return if in_bytes is 0 or -1. Instead
		 * skip the response and break out of the loop in an
		 * orderly fashion so that we can de-allocate the req
		 * and resp varaibles, and shutdown the socket. */
		if (in_bytes > 0) {
			res = add_to_queue(*req, the_queue, conn_params.policy);

			/* The queue is full if the return value is 1 */
			if (res) {
				struct response resp;
				/* Now provide a response! */
				memset(&resp, 0, sizeof(resp));
				resp.request_id = req->request.request_id;
				resp.ack = RESP_REJECTED;
				send(conn_socket, &resp, sizeof(struct response), 0);

				sync_printf("X%lu:%lf,%lf,%lf\n",
				       (unsigned long)req->request.request_id,
				       TSPEC_TO_DOUBLE(req->request.sent_timestamp),
				       TSPEC_TO_DOUBLE(req->request.request_length),
				       TSPEC_TO_DOUBLE(req->receipt_timestamp)
					);
			}
		}
	} while (in_bytes > 0);


	/* Stop all the worker threads. */
	control_workers(WORKERS_STOP, conn_params.workers, NULL);

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
	struct connection_params conn_params;
	conn_params.queue_size = 0;
	conn_params.workers = 1;
	conn_params.policy = QUEUE_FIFO;

	/* Parse all the command line arguments */
	while((opt = getopt(argc, argv, "q:w:p:")) != -1) {
		switch (opt) {
		case 'q':
			conn_params.queue_size = strtol(optarg, NULL, 10);
			printf("INFO: setting queue size = %ld\n", conn_params.queue_size);
			break;
		case 'w':
			conn_params.workers = strtol(optarg, NULL, 10);
			printf("INFO: setting worker count = %ld\n", conn_params.workers);
			break;
		case 'p':
			if (strcmp(optarg, "FIFO") == 0) {
				conn_params.policy = QUEUE_FIFO;
			} else if (strcmp(optarg, "SJN") == 0) {
				conn_params.policy = QUEUE_SJN;
			} else if (strcmp(optarg, "HSN") == 0) {
				fprintf(stderr, "ERROR: HSN policy is not implemented yet.\n");
				return EXIT_FAILURE;
			} else {
				ERROR_INFO();
				fprintf(stderr, "Invalid policy: %s\n", optarg);
				fprintf(stderr, USAGE_STRING, argv[0]);
				return EXIT_FAILURE;
			}
			printf("INFO: setting queue policy = %s\n", optarg);
			break;
		default: /* '?' */
			fprintf(stderr, USAGE_STRING, argv[0]);
		}

	}

	if (!conn_params.queue_size) {
		ERROR_INFO();
		fprintf(stderr, USAGE_STRING, argv[0]);
		return EXIT_FAILURE;
	}

	if (optind < argc) {
		socket_port = strtol(argv[optind], NULL, 10);
		printf("INFO: setting server port as: %d\n", socket_port);
	} else {
		ERROR_INFO();
		fprintf(stderr, USAGE_STRING, argv[0]);
		return EXIT_FAILURE;
	}

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

/* End of server_pol implementation version 6z8m4m1e3r3o6k */
