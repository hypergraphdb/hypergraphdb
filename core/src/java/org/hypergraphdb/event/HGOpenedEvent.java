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
 * A <code>HGOpenedEvent</code> is generated right after a new HyperGraph instance has been opened
 * and fully initialized. For example, if you create/open a HyperGraph with a call like this:
 * </p>
 * <p>
 * <code>
 * HyperGraph myGraph = new HyperGraph("/var/data/hg");
 * </code>
 * </p>
 * <p>
 * A <code>HGOpenedEvent</code> will generated and processed right before the <code>new</code> 
 * operator returns.
 * </p>
 * @author boris
 */
public class HGOpenedEvent extends HGEventBase 
{
}
