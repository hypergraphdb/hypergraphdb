/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * <p>
 * A filtering iterator wraps a source iterator an filter <strong>out</strong>
 * some of its elements based on a predicate.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 * @param <T>
 */
public class FilterIterator<T> implements Iterator<T> 
{
    private Iterator<T> iter;
    private Mapping<T, Boolean> ignore;
    private T lookahead;
    
    public FilterIterator(Iterator<T> iter, Mapping<T, Boolean> ignore)
    {
        this.iter = iter;
        this.ignore = ignore;
        this.lookahead = advance();
    }
    
    T advance()
    {
        while (iter.hasNext())
        {
            T n = iter.next();
            if (!ignore.eval(n))
                return n;
        }
        return null;
    }
    
    public boolean hasNext()
    {
        return lookahead != null;
    }
    
    public T next()
    {
        if (!hasNext()) 
            throw new NoSuchElementException();
        T x = lookahead;
        lookahead = advance();
        return x;
    }
    
    public void remove()
    {
        iter.remove();
    }    
}
