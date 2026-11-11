#ifndef __MD5SUMLIB_H__
#define __MD5SUMLIB_H__
/* DO NOT WRITE ANY CODE ABOVE THIS LINE*/

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

typedef uint32_t uint;
typedef uint8_t byte;
typedef uint64_t ulong;
typedef uint8_t uchar;

struct md5digest
{
	byte __digest [16];
};

#define print_digest(digest)						\
	do {								\
		int i;							\
		for(i = 0; i < 16; ++i)					\
			printf("%.2x", digest.__digest[i]);		\
		printf("\n");						\
	} while (0)

/* Compute the MD5 hash of a file given its pathname */
struct md5digest file_md5sum(const char * name);

/* Compute the MD5 hash of a memory buffer */
struct md5digest buf_md5sum(const char * orig_buf, size_t len);

/* DO NOT WRITE ANY CODE BEYOND THIS LINE*/
#endif

/* End of md5sum header version 0t4i4m2f5t6r2g */
