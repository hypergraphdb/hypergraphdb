/*
 * boostUUIDTest.cpp
 *
 *  Created on: Feb 23, 2010
 *      Author: amr
 */

#include <iostream>
#include <iomanip>
#include <vector>
#include <UUID.h>

using namespace std;

void print( const vector<byte> v )
{
        std::cout.fill( '0' );
        for( uint i=0 ; i<v.size( ) ; i++ )
        {
                std::cout << setw(8) << std::hex << (int)v[i];
                std::cout<< "-" ;
        }

        cout << endl;
}


int main( )
{
	UUID uuid;
	const vector<byte> v = uuid.getUUID_V4();
	print( v );
	return 0;
}
