#ifndef __PERFLIB_H__
#define __PERFLIB_H__

#include <linux/perf_event.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdint.h>

/* internally used wrapper around syscall to open performance counter. 
Returnss a file descriptor on success, returns -1 on failure. */
long perf_event_open(struct perf_event_attr *hw_event, pid_t pid, int cpu, int group_fd, unsigned long flags);

/* sets up a performance counter for specified hardware event
based on a <type> and <config> value. Returns a file descriptor (handle)
corresponding to this specified performance counter. 
NOTE: this must be called from the thread you wish to monitor */
int setup_perf_counter(uint64_t type, uint64_t config);

/*read the current value of the performance counter specified by a file descriptor. 
NOTE: you must use a call to ioctl() to RESET/ENABLE the performance counter prior to reading*/
uint64_t read_perf_counter(int fd);

#define PERFLIB_VER "6z6k9r1e2q4p3h"

#endif 
