/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.storage.lmdb;

import org.fusesource.lmdbjni.SecondaryDatabase;
import org.fusesource.lmdbjni.SecondaryKeyCreator;

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
    
		@Override
		public byte[] createSecondaryKey(SecondaryDatabase secondary, byte[] key, byte[] data) {
			return data;
		}
}
