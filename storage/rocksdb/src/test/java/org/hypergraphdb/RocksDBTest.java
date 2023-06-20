package org.hypergraphdb;

import org.hypergraphdb.storage.rocksdb.StorageImplementationRocksDB;
import org.hypergraphdb.util.HGUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;

public class RocksDBTest
{
    static HyperGraph graph;
    static String location = "./hgdblmdb";

    @BeforeClass
    public static void openGraph()
    {
        HGUtils.dropHyperGraphInstance(location);
        try
        {
            //location = Files.createTempDirectory(null).toString();
            HGConfiguration config = new HGConfiguration();
            config.setStoreImplementation(new StorageImplementationRocksDB());
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
        System.out.println((Object)graph.get(h.getPersistent()));
    }
}
