/*
 * RandomGenratorTest.cpp
 *
 *  Created on: Feb 7, 2010
 *      Author: amr
 */


#include <cstdlib>
#include <cstring>
#include <iostream>
#include <ctime>


#include "RandomGenerator.h"

int main( )
{
	AbsRandomGenerator *rndGen = new RandomGenerator( );
	std::cout << rndGen->getRandomNumber( 0 , 600 ) << std::endl;
	return 0;
}
