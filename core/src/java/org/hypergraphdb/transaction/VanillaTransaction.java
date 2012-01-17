/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.transaction;

/**
 * 
 * <p>
 * This is a <code>HGTransaction</code> implementation that only maintains
 * the attribute map. It is for use when transactions are disabled.
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class VanillaTransaction implements HGStorageTransaction
{
	
	public void abort() throws HGTransactionException
	{
	}

	public void commit() throws HGTransactionException
	{
	}
}
