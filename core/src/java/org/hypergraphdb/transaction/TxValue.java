/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.transaction;

/**
 * Represents a transactional value - transaction that do not modify this
 * value will see the last value committed at the time they were created
 * while a transaction that modifies the value will see what it wrote.  
 */
public class TxValue<T>
{
    private VBox<T> box;
    
    public TxValue(HGTransactionManager txManager, T initialValue)
    {
        this.box = new VBox<T>(txManager, initialValue);
    }
    
    /**
     * Return the value as of the time this transaction was created or 
     * as last written via the {@link TxValue#set(Object)} method.
     */
    public T get()
    {
        return box.get();
    }
    
    /**
     * Set a new value. The new value will only be visible to the current transaction
     * until committed.
     * 
     * @param value
     * @return The <code>value</code> argument.
     */
    public T set(T value)
    {
        box.put(value);
        return value;
    }
}
