package hgtest;

import java.util.List;

import org.hypergraphdb.HGConfiguration;
import org.hypergraphdb.HGEnvironment;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGIndex;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HGSortIndex;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.atom.AtomProjection;
import org.hypergraphdb.indexing.ByPartIndexer;
import org.hypergraphdb.query.AtomPartCondition;
import org.hypergraphdb.type.HGHandleType;
import org.hypergraphdb.type.Top;
import org.hypergraphdb.type.javaprimitive.LongType;
import org.hypergraphdb.type.javaprimitive.StringType;
import org.hypergraphdb.util.HGUtils;

import hgtest.atomref.TestHGAtomRef;
import hgtest.beans.Car;
import hgtest.beans.Person;
import hgtest.indexing.ManyToManyIndexerTests;
import hgtest.indexing.PropertyIndexingTests;
import hgtest.query.IndexEnumTypeTest;
import hgtest.query.Queries;
import hgtest.query.QueryCompilation;
import hgtest.tx.DataTxTests;
import hgtest.tx.NestedTxTests;
import hgtest.types.TestPrimitives;

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
                List<Object> children = hg
                        .getAll(graph, hg.typePlus(Top.class));
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
        
        HGSortIndex index = (HGSortIndex)graph.getStore().getIndex("ana", lt, ht, lt.getComparator(), true);
        
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

    public static void main(String[] argv)
    {
        //anatest();
        // TestQuery.go("/tmp/alain");
        PropertyIndexingTests test = new PropertyIndexingTests();
        test.setUp();
        try
        {
            test.testUpdateLiveAtom();
        }
        finally
        {
            test.tearDown();
        }
    }
}