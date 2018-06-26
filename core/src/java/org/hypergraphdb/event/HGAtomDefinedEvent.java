/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2018 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.event;

import org.hypergraphdb.HGHandle;

/**
 * <p>
 * This event is triggered after defining an atom was successfully completed. The
 * event is essentially an {@link HGAtomAddedEvent} so a listener uninterested in the 
 * fact that the atom was defined with an explicit handle can just work with the 
 * {@link HGAtomAddedEvent} interface. Note however that you must register to {@link HGAtomAddedEvent} 
 * and <code>HGAtomDefinedEvent</code>
 * separately.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public class HGAtomDefinedEvent extends HGAtomAddedEvent
{
	public HGAtomDefinedEvent(HGHandle handle, Object source)
	{
		super(handle, source);
	}
}
	