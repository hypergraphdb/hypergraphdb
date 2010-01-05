/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.maintenance;


/**
 * 
 * <p>
 * Represents an exception that occurred during the execution of a 
 * <code>MaintenanceOperation</code>. If the <code>fatal</code> flag is
 * set, it means that it is not advisable to try to perform other
 * maintenance operations (e.g. because of low-level DB failure). If the
 * flag is <code>false</code>, it means the maintenance operation failed
 * for some "higher level" reason, but it managed to clean up after itself
 * and other operations can be performed. 
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class MaintenanceException extends Exception
{
	private static final long serialVersionUID = -1;
	private boolean fatal;
	
	public MaintenanceException(boolean fatal, String msg)
	{
		super(msg);
	}
	
	public MaintenanceException(boolean fatal, String msg, Throwable cause)
	{
		super(msg, cause);
	}
	
	public boolean isFatal() { return fatal; }
	public void setFatal(boolean fatal) { this.fatal = fatal; }
}
