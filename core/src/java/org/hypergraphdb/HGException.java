/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb;

/**
 * 
 * <p>
 * A <code>HGException</code> is thrown by HyperGraph code anywhere an abnormal
 * situation, that cannot usually be handled in an obvious way besides
 * debugging, occurs.  
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class HGException extends RuntimeException
{
    static final long serialVersionUID = -1;
    
    public HGException(String msg)
    {
        super(msg);
    }
    
    public HGException(String msg, Throwable cause)
    {
        super(msg, cause);
    }
    
    public HGException(Throwable cause)
    {
        super(cause);
    }
}
