/*
 * AbsUUID.h
 *
 *  Created on: Feb 7, 2010
 *      Authors: Amr Shahin, Alaa Ibrahim
 */

#ifndef ABSUUID_H_
#define ABSUUID_H_

#include "common.h"
#include <vector>
#include <cstring>

abstract class AbsUUID
{
public :
	virtual const std::vector<byte> getUUID_V4( ) const = 0;
	static const int MAX_BYE_LEN = 16;
};

#endif /* ABSUUID_H_ */
