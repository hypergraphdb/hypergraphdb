package hgdbteststorage;

import hgtest.T;

import java.io.File;

import org.hypergraphdb.HGConfiguration;
import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.HGStore;
import org.hypergraphdb.storage.HGStoreImplementation;
import org.hypergraphdb.util.HGUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class StoreImplementationTestBase
{
	private static HGStoreImplementation impl;
	private static HGConfiguration config;
	
    public static String getGraphLocation()
    {
        return T.getTmpDirectory() /* "/home/borislav/data" */ + File.separator + "hgstoragetest"; 
    }
	
	@BeforeClass
	public static void initStorage()
	{
		HGUtils.dropHyperGraphInstance(getGraphLocation());
        impl = HGUtils.getImplementationOf(HGStoreImplementation.class.getName(), 
                						   "org.hypergraphdb.storage.bje.BJEStorageImplementation");
        config = new HGConfiguration();
        impl.startup(new HGStore(getGraphLocation(), config), config);
	}	
	
	@AfterClass
	public static void wipeStorage()
	{
		HGUtils.dropHyperGraphInstance(getGraphLocation());
	}
	
	public HGConfiguration config()
	{
		return config;
	}
	
	public HGHandleFactory hfactory()
	{
		return config().getHandleFactory();
	}
	
	public HGStoreImplementation impl()
	{
		return impl;
	}
}