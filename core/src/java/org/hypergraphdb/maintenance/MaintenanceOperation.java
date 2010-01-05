/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.maintenance;

import org.hypergraphdb.HyperGraph;

/**
 * 
 * <p>
 * Represents a maintenance operation performed on a HyperGraph database. Such operations
 * are presumed potentially very long and therefore should run in isolation, while there's 
 * no other activity being performed on the database. In addition, such operations must be 
 * resilient in case of interruption. They are like long, high-level transactions that are
 * expected to eventually complete. Completion doesn't necessarily mean success, but the operation
 * should guarantee a consistent, non-corrupted data once it's finished.   
 * </p>
 *
 * <p>
 * <code>MaintenanceOperation</code>s are created and scheduled to run upon the next time
 * the HyperGraph is opened. This is done simply by adding an instance of this interface
 * as a HyperGraph atom. HyperGraph will detect and run all maintenance operations the next 
 * time it is open. It is also possible to skip and/or cancel scheduled maintenance operations 
 * by setting the appropriate startup flags in the <code>HGConfiguration</code> used 
 * to open a database.  
 * </p>
 * 
 * <p>
 * In case of interruption (an abrupt program exit) that occurs during a maintenance operation,
 * the implementation is responsible to resume work where it left before. HyperGraph will ensure
 * that multiple scheduled maintenance operations are executed in the same order as they were added.
 * </p>
 *
 * <p>
 * It is possible to force execution of all scheduled maintenance operation at any point in time
 * by calling the <code>HyperGraph.runMaintenance()</code> method. However, an application must
 * make sure that no other threads are accessing the database and potentially causing inconsistent 
 * or corrupted data. 
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public interface MaintenanceOperation
{
	/**
	 * <p>
	 * Execute a maintenance operation.  
	 * </p>
	 * 
	 * @param graph The <code>HyperGraph</code> on which this maintenance operation is executed.
	 */
	void execute(HyperGraph graph) throws MaintenanceException;
}
