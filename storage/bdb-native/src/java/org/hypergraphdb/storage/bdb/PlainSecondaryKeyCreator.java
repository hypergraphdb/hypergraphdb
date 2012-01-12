/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.storage.bdb;

import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.SecondaryDatabase;
import com.sleepycat.db.SecondaryKeyCreator;

public class PlainSecondaryKeyCreator implements SecondaryKeyCreator
{
    private static final PlainSecondaryKeyCreator instance = new PlainSecondaryKeyCreator();
    
    private PlainSecondaryKeyCreator()
    {        
    }
    
    public static PlainSecondaryKeyCreator getInstance()
    {
        return instance;
    }
    
    public boolean createSecondaryKey(SecondaryDatabase secondary,
            DatabaseEntry key, DatabaseEntry data, DatabaseEntry result)
            throws DatabaseException
    {
        result.setData(data.getData());
        return true;
    }
}
