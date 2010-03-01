/*
 * UUID.cpp
 *
 *  Created on: Feb 23, 2010
 *      Author: amr
 */

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include "AbsUUID.h"
#include <vector>

class UUID : public AbsUUID
{
	public :
		UUID( ) ;
		virtual const std::vector<byte> getUUID_V4( ) const ;

	private:
		boost::uuids::uuid uuid;
};
