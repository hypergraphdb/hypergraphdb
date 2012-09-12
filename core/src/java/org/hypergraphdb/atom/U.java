/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.atom;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.HGLiveHandle;

// private utilities for this package. we don't want to expose them since they rely on internal
// implementation details...
class U 
{
	static HGPersistentHandle persistentHandle(HGHandle h)
	{
		if (h instanceof HGPersistentHandle)
			return (HGPersistentHandle)h;
		else
			return ((HGLiveHandle)h).getPersistent();
	}
	
	static byte [] getBytes(HGHandle h)
	{
		if (h instanceof HGPersistentHandle)
			return ((HGPersistentHandle)h).toByteArray();
		else
			return ((HGLiveHandle)h).getPersistent().toByteArray();
	}
	
	static final class HandleLink
	{
		HGHandle handle;
		HandleLink next;
		HandleLink(HGHandle handle, HandleLink next) 
			{ this.handle = handle; this.next = next; }
	}
}
