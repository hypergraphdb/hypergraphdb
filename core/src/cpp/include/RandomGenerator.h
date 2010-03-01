/*
 * RandomGenerator.h
 *
 *  Created on: Feb 7, 2010
 *      Authors: Amr Shahin, Alaa Ibrahim
 */

#ifndef RANDOMGENERATOR_H_
#define RANDOMGENERATOR_H_

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <ctime>
#include "AbsRandomGenerator.h"

class RandomGenerator : public AbsRandomGenerator {
    public:
        RandomGenerator();
        unsigned int getRandomNumber(unsigned int, unsigned int);
};

#endif /* RANDOMGENERATOR_H_ */
