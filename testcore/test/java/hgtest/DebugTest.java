package hgtest;

import hgtest.indexing.PropertyIndexingTests;

import java.util.List;

import org.hypergraphdb.HGConfiguration;
import org.hypergraphdb.HGEnvironment;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HGSortIndex;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.indexing.ByPartIndexer;
import org.hypergraphdb.type.HGHandleType;
import org.hypergraphdb.type.Top;
import org.hypergraphdb.type.javaprimitive.LongType;
import org.hypergraphdb.util.HGUtils;

public class DebugTest
{	
    public static class TestQuery
    {
        public static void go(String databaseLocation)
        {
            HyperGraph graph = null;

            try
            {
                HGConfiguration config = new HGConfiguration();
                config.setUseSystemAtomAttributes(false);
                graph = HGEnvironment.get(databaseLocation, config);
                List<Object> children = hg.getAll(graph, hg.typePlus(Top.class));
                System.out.println("size:" + children.size());
             }
            catch (Throwable t)
            {
                t.printStackTrace();
            }
            finally
            {
                if (graph != null)
                {
                    graph.close();
                }
            }
        }
    }

    public static void idxtest()
    {
        HGUtils.dropHyperGraphInstance("/tmp/anagraph");
        HyperGraph graph = HGEnvironment.get("/tmp/anagraph");
        
        LongType lt = graph.getTypeSystem().getAtomType(Long.class);
        HGHandleType ht = graph.getTypeSystem().getAtomType(HGHandle.class);
        
        HGSortIndex index = (HGSortIndex)graph.getStore().getIndex("ana", lt, ht, lt.getComparator(), null, true);
        
        HGPersistentHandle h = graph.getHandleFactory().makeHandle();
        
        graph.getTransactionManager().beginTransaction();
        index.addEntry(5l, h);
        graph.getTransactionManager().commit();
        graph.getTransactionManager().beginTransaction();
        System.out.println("Index size " + index.count());
        index.removeEntry(5l, h);
        System.out.println("Index size " + index.count());
        
        graph.getTransactionManager().commit();
    }   
    
    public static void anatest()
    {
        HGUtils.dropHyperGraphInstance("/tmp/anagraph");
        HyperGraph hyperGraph = HGEnvironment.get("/tmp/anagraph");
        
        HGHandle typeHandle = hyperGraph.getTypeSystem().getTypeHandle(Id.class);
        ByPartIndexer byPartIndexer = new ByPartIndexer("id_indexer", typeHandle, "id");
        hyperGraph.getIndexManager().register(byPartIndexer);
        hyperGraph.runMaintenance();

        HGSortIndex index = (HGSortIndex)hyperGraph.getIndexManager().getIndex(byPartIndexer);
        
//        for (long i = 1l; i < 10l; i++)
//        {
//            Id id = new Id(i);
//            hyperGraph.add(id);
//            id.setId(i);
//            hyperGraph.update(id);
//            id.setId(i);
//            hyperGraph.update(id);
//        }

        Id id = new Id(2);
        hyperGraph.add(id);
        id.setId(3);
        hyperGraph.update(id);
//        id.setId(i);
//        hyperGraph.update(id);
        
        HGSearchResult rs = index.findGT(0l);
        while (rs.hasNext())
            System.out.println("rs : " + rs.next());
        rs.close();
        List<Id> L = hyperGraph.getAll(hg.and(hg.type(Id.class), hg.gt("id", 0l)));
        System.out.println(L);
        System.out.println(L.size());
//        HGRandomAccessResult result = index.scanValues();
//        result.goBeforeFirst();
//        try
//        {
//            while (result.hasNext())
//            {
//                Id id = hyperGraph.get((HGHandle) result.next());
//                System.out.println(id.getId());
//            }
//        }
//        finally
//        {
//            result.close();
//        }
    }

    public static class Id
    {
        private long id;

        public Id(long id)
        {
            this.id = id;
        }

        public Id()
        {
        }

        public long getId()
        {
            return id;
        }

        public void setId(long id)
        {
            this.id = id;
        }
        
        public String toString() { return "ID[" + id + "]"; }
        	
    }
    
    /*
     * 
     * To run with native and incident annotations, add the following args.
     * -Djava.library.path=/home/borislav/hypergraphdb/testcore/target/lib -Dorg.hypergraphdb.storage.HGStoreImplementation=org.hypergraphdb.storage.bdb.BDBStorageImplementation -Dhgtest.GraphFactory=hgtest.annotatedincident.AnnotatedIncidentGraphFactory
     * 
     */
    public static void main(String[] argv)
    {
        //anatest();
//    	HGUtils.dropHyperGraphInstance("/tmp/alain");
//        TestQuery.go("/tmp/alain");
        PropertyIndexingTests test = new PropertyIndexingTests();
        test.setUp();
        
//        HGUtils.dropHyperGraphInstance(T.getTmpDirectory() + "/" + "emptyhg");
//        HyperGraph graph = HGEnvironment.get(T.getTmpDirectory() + "/" + "emptyhg");
//        HGHandle t1  = graph.add(40);
//        HGHandle lh = graph.add(new HGValueLink("edgeName", t1));
//        HGQuery q = HGQuery.make(HGHandle.class, graph);        
//        q.compile(hg.and(hg.eq("edgeName"), hg.incident(t1)));
//        Assert.assertEquals(q.findOne(), lh);
//        System.out.println(hg.getAll(graph, hg.type(String.class)));
//        graph.getTransactionManager().beginTransaction();
        try
        {
            //QueryCompile.parallel();
            test.derivedPropertyIndexing();
//            HGHandle h1 = graph.add("OR-TEST-1");
//            HGHandle h2 = graph.add("OR-TEST-2");
//            while (true)
//            {
//                System.out.println("...");
//            
//            HGSearchResult rs = null;                    
//                    //new AsyncSearchResultImpl(graph.find(hg.type(String.class)));
//            try
//            {
//                rs = graph.find(hg.or(hg.eq("OR-TEST-1"), hg.eq("OR-TEST-2")));
//                while (rs.hasNext())
//                {
//                    System.out.println(graph.get((HGHandle)rs.next()));
//                    //System.out.println(graph.get((HGHandle)((Future)rs.next()).get()));
//                }            
//            }
//            finally
//            {
//                if (rs != null)
//                    rs.close();
//            }
//            }
        }
        catch (Throwable t)
        {
            t.printStackTrace(System.err);
        }
        finally
        {
            test.tearDown();
//            try { graph.getTransactionManager().endTransaction(false); }
//            catch (Throwable t) { t.printStackTrace(); }
        }
    }
}