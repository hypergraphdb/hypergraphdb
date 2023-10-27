package hgtest.lmdb;

import java.io.File;


import org.hypergraphdb.HGConfiguration;
import org.hypergraphdb.HGEnvironment;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.storage.lmdb.HGByteArrayBufferProxyLMDB;
import org.hypergraphdb.storage.lmdb.StorageImplementationLMDB;
import org.hypergraphdb.util.HGUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lmdbjava.ByteArrayProxy;

public class LmdbBasicTests
{
	static HyperGraph graph;
	static String location = "/home/borislav/temp/hglmdb_test";
	
	@BeforeClass
	public static void openGraph()
	{
		HGUtils.dropHyperGraphInstance(location);
		try
		{
			//location = Files.createTempDirectory(null).toString();
			HGConfiguration config = new HGConfiguration();
			config.setStoreImplementation(
			        new StorageImplementationLMDB<byte[]>(ByteArrayProxy.PROXY_BA, 
	                        new HGByteArrayBufferProxyLMDB(config.getHandleFactory())));			        
//			        new StorageImplementationLMDB());
			new File(location).mkdirs();
			graph = HGEnvironment.get(location, config);
			System.out.println("Graph " + graph.getLocation() + " opened.");
		}
		catch (Throwable t)
		{
			t.printStackTrace(System.err);
		}
	}
	
	@AfterClass
	public static void closeGraph()
	{
		graph.close();
	}
	
	@Test
	public void addRetrieveAtom()
	{
		HGHandle h = graph.add("test");
		System.out.println((Object)graph.get(h.getPersistent()));
	}
}
