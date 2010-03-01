/*
 * UUID.cpp
 *
 *  Created on: Feb 23, 2010
 *      Author: amr
 */

#include "UUID.h"

UUID::UUID()
:uuid( boost::uuids::random_generator()() )
{
}

const std::vector<byte> UUID::getUUID_V4( ) const
{
	std::vector<byte> v( uuid.size() ) ;
	std::copy( this->uuid.begin() , this->uuid.end() , v.begin() ) ;
	return v ;
}
