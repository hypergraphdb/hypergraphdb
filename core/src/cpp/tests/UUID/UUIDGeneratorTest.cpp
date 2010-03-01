/*
 * UUIDGeneratorTest.cpp
 *
 *  Created on: Feb 7, 2010
 *      Author: amr
 */

#include "SSLRandomGenerator.h"
#include "UUIDGenerator.h"
#include <iomanip>

int main( )
{
	AbsRandomGenerator *randomGen = new SSLRandomGenerator( );
	UUIDGenerator uuidGen = UUIDGenerator( randomGen );
	std::vector<byte> uuid = uuidGen.getUUID_V4() ;
	std::cout.fill( '0' );
	for( int i =0 ; i< AbsUUID::MAX_BYE_LEN ; i++ )
	{
		std::cout << std::setw(8) << std::hex << (int)uuid[i] ;
		std::cout<< "-" ;
	}

	std::cout << std::endl;
}
