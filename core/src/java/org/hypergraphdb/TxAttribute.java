/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb;

import org.hypergraphdb.transaction.HGTransactionManager;

class TxAttribute
{
	static String IN_REMOVAL = "ATOMS_IN_REMOVAL";
	
	@SuppressWarnings("unchecked")
	static <T> T getSet(HGTransactionManager tm, String name, Class<T> def)
	{
		T x = (T)tm.getContext().getCurrent().getAttribute(name);
		if (x == null)
		{
			try { x = def.newInstance(); }
			catch (Exception ex) { throw new RuntimeException(ex); }
			tm.getContext().getCurrent().setAttribute(name, x);
		}
		return x;
	}
	
	static void remove(HGTransactionManager tm, String name)
	{
		tm.getContext().getCurrent().removeAttribute(name);
	}
}
