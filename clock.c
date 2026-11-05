#include <stdio.h>
#include <string.h>
#include <time.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>

#include "timelib.h"

int main(int argc, char *argv[]) {

    if (argc != 4) {
        fprintf(stderr, "Usage: %s <seconds> <nanoseconds> <s|b>\n", argv[0]);
        return EXIT_FAILURE;
    }

    long sec = atol(argv[1]);
    long nsec = atol(argv[2]);
    char input = argv[3][0];

    const char *method;
    uint64_t elapsed_clocks;

    if (input == 's') {
        method = "SLEEP";
        elapsed_clocks = get_elapsed_sleep(sec, nsec);
    } else if (input == 'b') {
        method = "BUSYWAIT";
        elapsed_clocks = get_elapsed_busywait(sec, nsec);
    } else {
        fprintf(stderr, "Invalid input. Enter 's' for sleep or 'b' for busywait.\n");
        return EXIT_FAILURE;
    }

    double elapsed_sec = sec + nsec / 1e9;
    double clock_speed_mhz = (elapsed_clocks / elapsed_sec) / 1e6;

    printf("WaitMethod: %s\n", method);
    printf("WaitTime: %ld %ld\n", sec, nsec);
    printf("ClocksElapsed: %llu\n", (unsigned long long)elapsed_clocks);
    printf("ClockSpeed: %f\n", clock_speed_mhz);

    return EXIT_SUCCESS;
}