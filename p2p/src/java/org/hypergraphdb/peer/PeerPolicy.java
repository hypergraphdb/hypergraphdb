/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer;

/**
 * @author Cipri Costa
 * 
 * The interface is used to implement partitioning ... but it is just a first version. 
 * Will probably be removed soon.
 */
public interface PeerPolicy
{
	boolean shouldStore(Object atom);
}
