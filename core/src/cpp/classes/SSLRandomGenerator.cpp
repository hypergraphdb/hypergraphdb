/*
 * SSLRandomGenerator.cpp
 *
 *  Created on: Feb 15, 2010
 *      Author: amr
 */

#include <openssl/rand.h>
#include "SSLRandomGenerator.h"

unsigned int SSLRandomGenerator::getRandomNumber( unsigned int iMin , unsigned int iMax )
{
	unsigned int buf;
	RAND_bytes((unsigned char *)&buf,sizeof(unsigned int));
	return buf % (iMin -iMax) + iMin;
}
