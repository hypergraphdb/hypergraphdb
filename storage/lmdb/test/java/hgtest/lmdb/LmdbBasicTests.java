package hgtest.lmdb;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.hypergraphdb.HGConfiguration;
import org.hypergraphdb.HGEnvironment;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.storage.lmdb.LmdbStorageImplementation;
import org.hypergraphdb.util.HGUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class LmdbBasicTests
{
	static HyperGraph graph;
	static String location = "/Users/borislav/temp/hglmdb_test";
	
	@BeforeClass
	public static void openGraph()
	{
		HGUtils.dropHyperGraphInstance(location);
		try
		{
			//location = Files.createTempDirectory(null).toString();
			HGConfiguration config = new HGConfiguration();
			config.setStoreImplementation(new LmdbStorageImplementation());
			new File(location).mkdirs();
			graph = HGEnvironment.get(location, config);
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
		System.out.println(graph.get(h.getPersistent()));
	}
}
