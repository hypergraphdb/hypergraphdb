/*
 * UUIDGenerator.h
 *
 *  Created on: Feb 7, 2010
 *      Author: amr
 */

#include <cstdlib>
#include <cstring>
#include <sstream>
#include <iostream>
#include <ctime>
#include "AbsUUID.h"
#include "AbsRandomGenerator.h"

#ifndef UUIDGENERATOR_H_
#define UUIDGENERATOR_H_

class UUIDGenerator: public AbsUUID
{
protected:
	AbsRandomGenerator *pRandGenerator;
public:
	UUIDGenerator( AbsRandomGenerator *pRndGen );
	const std::vector<byte> getUUID_V4( ) const ;


};

#endif /* UUIDGENERATOR_H_ */
