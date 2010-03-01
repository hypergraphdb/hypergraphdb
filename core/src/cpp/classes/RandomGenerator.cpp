/*
 * RandomGenerator.cpp
 *
 *  Created on: Feb 7, 2010
 *      Authors: Amr shahin, Alaa Ibrahim
 */

#include "RandomGenerator.h"

RandomGenerator::RandomGenerator()
{
	//change the std::random seed
    std::srand ( std::time(NULL) );
}


unsigned int RandomGenerator::getRandomNumber(unsigned int uiMin, unsigned int uiMax)
{
    unsigned int uiRandom = (unsigned int)std::rand();
    return uiRandom % (uiMax -uiMin) + uiMin;
}
