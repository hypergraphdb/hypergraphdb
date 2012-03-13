/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */

package org.hypergraphdb.storage.redis;
import org.hypergraphdb.HGRandomAccessResult;

/**
 * <p>
 * An <code>IndexResultSet</code> is based on a cursor over an indexed set of values.
 * Implementation of complex query execution may move the cursor position based on some
 * index key to speed up query processing.  
 * </p>
 * 
 * @author Borislav Iordanov
 */
@SuppressWarnings("unchecked")
public abstract class JIndexResultSet<T> implements HGRandomAccessResult<T>
{        
    protected  void closeNoException()      {         try { close(); } catch (Throwable t) { }      }
    protected  void checkCursor()  { }
    protected JIndexResultSet() {    }
    public abstract void goBeforeFirst();
    public abstract void goAfterLast();
    public abstract GotoResult goTo(T value, boolean exactMatch);
    public abstract void close();
    public abstract T current();
    public abstract boolean hasPrev();
    public abstract boolean hasNext();
    public abstract T prev();
    public abstract T next();
    public void remove()
    {   throw new UnsupportedOperationException( "HG - JIndexResultSet does not implement remove."); }
    public abstract int count();
    public abstract void removeCurrent();
}
