/*
 * UUIDGenerator.cpp
 *
 *  Created on: Feb 7, 2010
 *      Authors: Amr Shahin, Alaa Ibrahim
 */

#include "UUIDGenerator.h"
#include <iomanip>

UUIDGenerator::UUIDGenerator( AbsRandomGenerator *pRndGen )
	:pRandGenerator( pRndGen )
{

}

const std::vector<byte>  UUIDGenerator::getUUID_V4( ) const
{
	std::vector<byte> bUUIDOutput(UUIDGenerator::MAX_BYE_LEN) ;
	//first six bytes
	bUUIDOutput[0] = pRandGenerator->getRandomNumber(0,0xff);
	bUUIDOutput[1] = pRandGenerator->getRandomNumber(0,0xff);
	bUUIDOutput[2] = pRandGenerator->getRandomNumber(0,0xff);
	bUUIDOutput[3] = pRandGenerator->getRandomNumber(0,0xff);
	bUUIDOutput[4] = pRandGenerator->getRandomNumber(0,0xff);
	bUUIDOutput[5] = pRandGenerator->getRandomNumber(0,0xff);
	//7th byte
	bUUIDOutput[6] = pRandGenerator->getRandomNumber(0,0x0f) | 0x40 ;

	//8th byte
	bUUIDOutput[7] = pRandGenerator->getRandomNumber(0,0xff);
	//9th
	bUUIDOutput[8] = pRandGenerator->getRandomNumber(0,0x3f) | 0x80;

	bUUIDOutput[9] = pRandGenerator->getRandomNumber(0,0xff);
	bUUIDOutput[10] = pRandGenerator->getRandomNumber(0,0xff);
	bUUIDOutput[11] = pRandGenerator->getRandomNumber(0,0xff);
	bUUIDOutput[12] = pRandGenerator->getRandomNumber(0,0xff);
	bUUIDOutput[13] = pRandGenerator->getRandomNumber(0,0xff);
	bUUIDOutput[14] = pRandGenerator->getRandomNumber(0,0xff);
	bUUIDOutput[15] = pRandGenerator->getRandomNumber(0,0xff);

	return bUUIDOutput;
}
