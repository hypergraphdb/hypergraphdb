/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer;


import java.io.File;
import mjson.Json;

public class StartPeer
{
    private static void die(String msg)
    {
        System.out.println(msg);
        System.out.println("Syntax: StartPeer <configfile> [db=<graph location>] [name=<peer name>] [port=<peer port>.");
        System.out.println("where configfile is a JSON formatted configuration file " +
                           "and the optional 'db', 'name' and 'port' parameters overwrite the " +
                           "'localDB', 'peerName' and 'tcp port' confuration parameters.");
        System.exit(-1);        
    }
    
	public static void main(String[] args)
	{
		if (args.length < 1)
		    die("No arguments.");
		
		String filename = args[0];
		String db = null;
		String name = null;
		String port = null;
		for (int i = 1; i < args.length; i++)
		{
		    String [] A = args[i].split("=");
		    if (A.length != 2)
		        die("Invalid argument " + args[i]);
		    if ("db".equals(A[0]))
		        db = A[1];
		    else if ("name".equals(A[0]))
		        name = A[1];
		    else if ("port".equals(A[0]))
		        port = A[1];
		    else
		        die("Invalid parameter name " + A[0]);
		}
		Json configuration = HyperGraphPeer.loadConfiguration(new File(filename));
		if (db != null)
		    configuration.set("localDB", db);
		if (name != null)
		    configuration.set("peerName", name);
		if (port != null)
		    configuration.at("jxta").at("tcp").set("port", Integer.parseInt(port));
        HyperGraphPeer server = new HyperGraphPeer(configuration);
        server.start();
        while (true)
        	try { Thread.sleep(5000); } catch (InterruptedException ex) { break; }
	}
}