/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb;

import java.util.NoSuchElementException;

/**
 * <p>
 * </p>
 * 
 * @author Borislav Iordanov
 */
class EmptySearchResult implements HGRandomAccessResult<Object> 
{
	public void close() { }
	public Object current() { throw new NoSuchElementException("This is an emtpy HGSearchResult"); }
	public boolean hasPrev() { return false; }
	public Object prev() { throw new NoSuchElementException("This is an emtpy HGSearchResult"); }
	public boolean hasNext() { return false; }
	public Object next() { throw new NoSuchElementException("This is an emtpy HGSearchResult"); }
	public void remove() { new UnsupportedOperationException("This is an emtpy HGSearchResult");}
	public boolean isOrdered() { return true; }
	public GotoResult goTo(Object x, boolean exactMatch) { return GotoResult.nothing; }
	public void goBeforeFirst() { }
	public void goAfterLast() { }
}