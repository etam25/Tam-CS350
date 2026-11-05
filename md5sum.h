/*******************************************************************************
* MD5 Cryptographic Hash Computation Library (header)
*
* Description: A simple library to handle the calculation of MD5
*     hashes. The code has been adapted from the NetLib project
*     (https://www.netlib.org/). It has been modified to export the
*     md5sum function without a main function. The original comment is
*     kept below to provide appropriate credit to the original
*     creator.
*
* Author:
*     Renato Mancuso <rmancuso@bu.edu>
*
* Affiliation:
*     Boston University
*
* Creation Date:
*     October 30, 2023
*
* Last Update:
*     October 23, 2026
*
* Notes:
*     Ensure to link against the necessary dependencies when compiling and
*     using this library. Modifications or improvements are welcome. Please
*     refer to the accompanying documentation for detailed usage instructions.
*
*******************************************************************************/

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
