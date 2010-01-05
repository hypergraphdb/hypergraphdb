/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.transaction;

import java.util.Iterator;

/**
 * 
 * <p>
 * Represents a transaction object. The interface is minimalistic on purpose, exposing
 * only the two essential operations one can do one a transaction.
 * </p>
 *
 * <p>
 * Each transaction can carry an arbitrary set of attributes along with it. This is useful 
 * for attaching contextual information to transactions without intruding to otherwise simple
 * APIs. When a transaction is committed or aborted, all attributes are removed. 
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public interface HGTransaction
{
	void setAttribute(String name, Object value);
	Object getAttribute(String name);
	void removeAttribute(String name);
	Iterator<String> getAttributeNames();
	void commit() throws HGTransactionException;
	void abort() throws HGTransactionException;
}
