/*
 * AbsRandomGenerator.h
 *
 *  Created on: Feb 7, 2010
 *      Authors: Amr Shahin, Alaa Ibrahim
 */

#ifndef ABSRANDOMGENERATOR_H_
#define ABSRANDOMGENERATOR_H_

#include "common.h"

abstract class AbsRandomGenerator
{
public:
	virtual unsigned int getRandomNumber( unsigned int uiMin, unsigned int uiMax ) = 0;
};

#endif /* ABSRANDOMGENERATOR_H_ */
