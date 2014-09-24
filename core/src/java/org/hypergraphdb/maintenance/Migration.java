package org.hypergraphdb.maintenance;

import org.hypergraphdb.HGEnvironment;
import org.hypergraphdb.HGStore;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.util.HGDatabaseVersionFile;

public class Migration 
{
	public static void recreateIndex(HGStore store, String indexname)
	{
		
	}
	
	public static void move11to13(String dblocation)
	{
		HGDatabaseVersionFile vf = HGEnvironment.getVersions(dblocation);
		if (vf.getVersion("hgdb") != null)
			throw new RuntimeException("Target database has version " + vf.getVersion("hgdb"));
	}
	
	public static void main(String [] argv)
	{
		HyperGraph graph = HGEnvironment.get("/tmp/testhgdbversion");
//		graph.close();
		move11to13("/home/borislav/niches/test");		
	}
}
