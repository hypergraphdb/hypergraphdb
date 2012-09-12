/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.event;

/**
 * <p>
 * This interface marks an object that is a HyperGraph event. HyperGraph
 * events represent various HyperGraph activities, such as adding, removing or
 * simply accessing atoms. Listeners can be registered with the <code>HGEventManager</code>
 * bound to a <code>HyperGraph</code> instance.
 * </p>  
 *
 */
public interface HGEvent 
{
    public Object getSource();
}