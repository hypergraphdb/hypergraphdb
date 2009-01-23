package org.hypergraphdb.peer;

import java.io.File;

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
        server.start("user", "pwd");
        while (true)
        	try { Thread.sleep(5000); } catch (InterruptedException ex) { break; }
	}
}