/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb;

import java.io.File;
import java.util.*;

/**
 * <p>
 * This class provides some facilities to manage several open HyperGraph databases
 * within a single virtual machine. This is useful when one needs to access a
 * currently open database by its location.   
 * </p>
 * 
 * <p>
 * The class essentially implements a static map of currently open databases. The
 * general name <code>HGEnvironment</code> reflects the intent to eventually
 * put JVM-wide operations here.  
 * </p>
 * 
 * @author Borislav Iordanov
 */
public  class HGEnvironment 
{		
	private static Map<String, HyperGraph> dbs = new HashMap<String, HyperGraph>();
	private static Map<String, HGConfiguration> configs = new HashMap<String, HGConfiguration>();
	
	synchronized static void set(String location, HyperGraph graph)
	{
		dbs.put(location, graph);
	}
	
	synchronized static void remove(String location)
	{
		dbs.remove(location);
	}
	
	static String normalize(String location)
	{
		char last = location.charAt(location.length() - 1); 
		if (last == '/' || last == '\\')
			location = location.substring(0, location.length() - 1);
		return location;
	}
	
	/**
	 * <p>
	 * Retrieve an already opened or open a HyperGraph instance. Note that a new
	 * database instance will potentially be created via <code>new HyperGraph(location)</code>
	 * if it doesn't already exist.
	 * </p>
	 * 
	 * @param location The location of the HyperGraph instance.
	 * @return The HyperGraph database instance.
	 */
	public synchronized static HyperGraph get(String location) 
	{ 
		location = normalize(location);
		HyperGraph hg = dbs.get(location);
		if (hg == null)
		{
			hg = new HyperGraph();
			hg.setConfig(getConfiguration(location));
			hg.open(location);
			dbs.put(location, hg);
		}
		else if (!hg.isOpen())
		{
			if (configs.containsKey(location))
				hg.setConfig(configs.get(location));
			hg.open(location);
		}
		return hg;
	}
	
	/**
	 * <p>Retrieve the HyperGraphDB instance at the specified location and open it
	 * (if not already opened) with the given configuration. If the instance has
	 * already been opened, the configuration parameter is ignored.
	 * 
	 * @param location The filesystem path of the database instance.
	 * @param config The set of configuration parameters.
	 * @return
	 */
	public synchronized static HyperGraph get(String location, HGConfiguration config) 
	{ 
		location = normalize(location);
		configs.put(location, config);
		return get(location);
	}
	
	/**
	 * <p>
	 * Same as <code>get</code>, but will return <code>null</code> if there is
	 * no database at that location.
	 * </p>
	 */
	public synchronized static HyperGraph getExistingOnly(String location)
	{
		location = normalize(location);
		HyperGraph hg = dbs.get(location);
		if (hg == null)
		{
			if (exists(location))
				hg = new HyperGraph(location);
		}
		return hg;
	}
	
	/**
	 * <p>
	 * Return <code>true</code> if there is a HyperGraph database at the given location 
	 * and <code>false</code> otherwise.
	 * </p> 
	 */	
	public synchronized static boolean exists(String location)
	{
		return new File(location, "hgstore_idx_HGATOMTYPE").exists();
	}
	
	/**
	 * <p>
	 * Return <code>true</code> if the database at the given location is already
	 * open and <code>false</code> otherwise.
	 * </p> 
	 */
	public synchronized static boolean isOpen(String location)
	{
		return dbs.containsKey(location);
	}
	
	/**
	 * <p>
	 * Configure a HyperGraphDB instance before it is actually opened. If the instance
	 * at that location is already opened, the new configuration will only take effect
	 * if you close and re-open the instance. 
	 * </p>
	 * 
	 * @param location The filesystem path to the database instance.
	 * @param config A <code>HGConfiguration</code> with the desired parameters set.
	 */
	public synchronized static void configure(String location, HGConfiguration config)
	{
		configs.put(location, config);
	}
	
	/**
	 * <p>
	 * Retrieve the configuration of a HyperGraphDB instance. If no configuration was
	 * previously defined, a new one will be created.
	 * </p>
	 * 
	 * @param location The filesystem path to the HyperGraphDB instance.
	 */
	public synchronized static HGConfiguration getConfiguration(String location)
	{
		location = normalize(location);
		HGConfiguration conf = configs.get(location);
		if (conf == null)
		{
			HyperGraph hg = dbs.get(location);
			if (hg != null)
				conf = hg.getConfig();
			else
				conf = new HGConfiguration();
			configs.put(location, conf);
		}
		return conf;
	}
	
	// Try to make sure all HyperGraphs are properly closed during shutdown.
	private static class OnShutdown implements Runnable
	{
		public void run()
		{
			for (HyperGraph graph : dbs.values())
			{
				if (graph.isOpen())
					try { graph.close(); } 
					catch (Throwable t) 
					{ 
						System.err.println("Problem closing HyperGraphDB instance at " + 
										   graph.getLocation() + ", stack trace follows...");
						t.printStackTrace(System.err);						
					}
			}
		}
	}	
	
	static
	{
		Runtime.getRuntime().addShutdownHook(new Thread(new OnShutdown()));
	}
}
