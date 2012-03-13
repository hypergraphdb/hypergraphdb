package hgtest.storage;

import hgtest.storage.*;
import org.hypergraphdb.*;
import org.hypergraphdb.storage.HGStoreImplementation;
import org.hypergraphdb.storage.bdb.BDBStorageImplementation;
import org.hypergraphdb.storage.redis.JedisStorImpl;

import java.util.*;

class TestWrapper {

    private static HyperGraph graph;
    //private static final List<String> stringList = TestCommons.javaRandomStringList(100);
    static Map<String, HGPersistentHandle> linkStringMap;
    private static List<HGPersistentHandle> dataHandleList;
    private final String[] iniArgs;

    public TestWrapper(String[] args) {
        this.iniArgs= args;
    }
    //static scala.util.Random random = new scala.util.Random();


    public static void main(String[] args){
        TestWrapper tjh = new TestWrapper(args);
        tjh.run(args);
    }

    public void run(String[] args)
    {
        // indicate as args: 1) host 2) port 3) "bdb" or redis 4) Size of Testdata
        if(args.length >2 && args[2].equals("bdb"))
            graph = initializeGraph(true, args);
        else 
            graph = initializeGraph(false, args);
        
        if(args.length >3 &&  args[3] != null )
            try { TestCommons.setDataSize(Integer.valueOf(args[3])); } catch (NumberFormatException e) {}

//        dataHandleList = storeData(graph, stringList);

        scala.collection.immutable.Map emptyMap = new scala.collection.immutable.HashMap<String, Object>();   //empty<String,Object>();

        new IndexTest2().setGraph(graph).execute(null, emptyMap, true, true, true, true, true);
        new BiDirIndexTest().setGraph(graph).execute(null, emptyMap, true, true, true, true, true);
 //       new test.JHGDB.LinkTests().setGraph(graph).execute(null, emptyMap, true, true, true, true, true);
        new TestStoreImpl().setGraph(graph).execute(null, emptyMap, true, true, true, true, true);

        graph.close();
        System.out.println("Tests completed");
    }



    private static HyperGraph initializeGraph(boolean bdb, String[] args){
        HGConfiguration config = new HGConfiguration();
        if(!bdb)
        {   config.setStoreImplementation(new JedisStorImpl());
            config.setTransactional(false);
            graph = new HyperGraph();
            graph.setConfig(config);
            if(args.length >0)
                graph.open(args[0]);
            else 
                 graph.open("localhost");
        }

        else
        {
            config.setStoreImplementation((HGStoreImplementation) new BDBStorageImplementation());
            graph = new HyperGraph("/home/ingvar/workspace/hgdb2/mixstordbdir");
            config.setTransactional(true);
            graph.setConfig(config);
        }

        return graph;
    }


}
