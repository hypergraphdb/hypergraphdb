package org.hypergraphdb.peer;

import java.io.File;

import org.hypergraphdb.query.AnyAtomCondition;

public class StartPeer
{
	public static void main(String[] args)
	{
		if (args.length < 1)
		{
			System.out.println("Syntax: StartPeer <configfile>.");
			System.exit(-1);
		}
		
        HyperGraphPeer server = new HyperGraphPeer(new File(args[0]));
        if (server.start("user", "pwd"))        
        {                
        	//peer is started ...                
        	//set atom interests ...                
        	server.setAtomInterests(new AnyAtomCondition());                
        	//catch up ...                
        	server.catchUp();          		
        }
        while (true)
        	try { Thread.sleep(5000); } catch (InterruptedException ex) { break; }
	}
}
