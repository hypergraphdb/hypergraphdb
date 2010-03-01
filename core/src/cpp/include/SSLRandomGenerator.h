/*
 * SSLRandomGenerator.h
 *
 *  Created on: Feb 15, 2010
 *      Author: amr
 */

#ifndef SSLRANDOMGENERATOR_H_
#define SSLRANDOMGENERATOR_H_

#include "AbsRandomGenerator.h"

class SSLRandomGenerator : public AbsRandomGenerator {
    public:
		//SSLRandomGenerator();
        unsigned int getRandomNumber(unsigned int, unsigned int);
};

#endif /* SSLRANDOMGENERATOR_H_ */
