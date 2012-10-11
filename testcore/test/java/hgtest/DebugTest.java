package hgtest;

import java.util.List;

import org.hypergraphdb.HGConfiguration;
import org.hypergraphdb.HGEnvironment;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGIndex;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.atom.AtomProjection;
import org.hypergraphdb.indexing.ByPartIndexer;
import org.hypergraphdb.query.AtomPartCondition;
import org.hypergraphdb.type.Top;
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

    public static void anatest()
    {
        HGUtils.dropHyperGraphInstance("/tmp/anagraph");
        HyperGraph hyperGraph = HGEnvironment.get("/tmp/anagraph");


        HGHandle typeHandle = hyperGraph.getTypeSystem().getTypeHandle(Id.class);
        ByPartIndexer byPartIndexer = new ByPartIndexer("id_indexer", typeHandle, "id");
        hyperGraph.getIndexManager().register(byPartIndexer);
        hyperGraph.runMaintenance();

        for (long i = 1l; i < 20000l; i++)
        {
            hyperGraph.add(new Id(i));
        }

        HGIndex index = hyperGraph.getIndexManager().getIndex(byPartIndexer);
        HGRandomAccessResult result = index.scanValues();
        result.goBeforeFirst();
        try
        {
            while (result.hasNext())
            {
                Id id = hyperGraph.get((HGHandle) result.next());
                System.out.println(id.getId());
            }
        }
        finally
        {
            result.close();
        }
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
    }

    public static void main(String[] argv)
    {
        // TestQuery.go("/tmp/alain");
        IndexEnumTypeTest test = new IndexEnumTypeTest();
        test.setUp();
        try
        {
            test.testEnumIndex();
        }
        finally
        {
            test.tearDown();
        }
    }
}