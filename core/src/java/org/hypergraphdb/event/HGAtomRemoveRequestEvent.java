/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.event;

import org.hypergraphdb.HGHandle;

/**
 * <p>
 * A <code>HGAtomRemoveRequestEvent</event> is triggered when an attempt is made to remove
 * an from HyperGraph, but before the removal process proceeds. This event gives a chance to 
 * an application to cancel the removal of an atom. A listener to this event may return
 * the <code>HGListener.Result.cancel</code> code which will prevent the removal from happening.
 * On the other hand, if an application needs to perform some action as a result to an already 
 * completed removal, the application should then listen to the <code>HGAtomRemovedEvent</code>. 
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class HGAtomRemoveRequestEvent extends HGAtomEvent 
{
	/**
	 * <p>Construct a new <code>HGAtomRemoveRequestEvent</code> for the
	 * given atom.</p>
	 * 
	 * @param handle The <code>HGHandle</code> of the atom.
	 */
	public HGAtomRemoveRequestEvent(HGHandle handle)
	{
		super(handle);
	}
}
