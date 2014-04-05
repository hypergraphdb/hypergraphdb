package hgtest.indexing;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGIndex;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.indexing.HGIndexer;
import org.hypergraphdb.query.IndexCondition;
import org.hypergraphdb.storage.BAtoHandle;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.testng.Assert;
import org.testng.annotations.Test;

import hgtest.HGTestBase;

public class ManyToManyIndexerTests extends HGTestBase
{
    public static class Atom
    {
        private Set<HGHandle> handles = new HashSet<HGHandle>();
        private int idx = 0;

        public Atom()
        {
        }

        public Atom(int idx)
        {
            this.idx = idx;
        }

        public int getIdx()
        {
            return idx;
        }

        public void setIdx(int idx)
        {
            this.idx = idx;
        }

        public Set<HGHandle> getHandles()
        {
            return handles;
        }

        public void setHandles(Set<HGHandle> handles)
        {
            this.handles = handles;
        }
        
        public String toString()
        {
            return "Atom-" + idx + " " + handles;
        }
    }

    // Index x -> atomHandle for each x in atom.handles
    public static class ManyToManyIndexer implements HGIndexer
    {
        private HGHandle type;

        public Comparator<?> getComparator(HyperGraph graph)
        {
            return null; // use default byte[]comparator
        }

        public ByteArrayConverter<?> getConverter(HyperGraph graph)
        {
            return BAtoHandle.getInstance(graph.getHandleFactory());
        }

        public HGHandle getType()
        {
            return type;
        }

        public void setType(HGHandle type)
        {
            this.type = type;
        }

        @SuppressWarnings("unchecked")
        public void index(HyperGraph graph, HGHandle atomHandle, Object atom,
                          HGIndex index)
        {
            Atom a = (Atom) atom;
            for (HGHandle h : a.getHandles())
            {
                index.addEntry(graph.getPersistentHandle(h), graph
                        .getPersistentHandle(atomHandle));
            }
        }

        @SuppressWarnings("unchecked")
        public void unindex(HyperGraph graph, HGHandle atomHandle, Object atom,
                            HGIndex index)
        {
            Atom a = (Atom) atom;
            for (HGHandle h : a.getHandles())
            {
                index.removeEntry(graph.getPersistentHandle(h), graph
                        .getPersistentHandle(atomHandle));
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testHandleSetProperty()
    {
        HGIndexer indexer = new ManyToManyIndexer();
        indexer.setType(graph.getTypeSystem().getTypeHandle(Atom.class));
        graph.getIndexManager().register(indexer);
        graph.runMaintenance();
        for (int i = 0; i < 1000; i++)
        {
            graph.add(i);
        }
        HashMap<Integer, List<Atom>> ramIndex = new HashMap<Integer, List<Atom>>();
        for (int i = 1; i <= 40; i++)
        {
            Atom a = new Atom(i);
            for (int j = 0; j < 25; j++)
            {
                List<Atom> L = ramIndex.get(i * j);
                if (L == null)
                {
                    L = new ArrayList<Atom>();
                    ramIndex.put(i * j, L);
                }
                if (!L.contains(a))
                {
                    a.getHandles().add(
                            (HGHandle) hg.findOne(graph, hg.eq(i * j)));
                    L.add(a);
                }
                else
                    System.out.println("Already have " + i + " with " + j);
            }
            graph.add(a);
        }
        HGIndex index = graph.getIndexManager().getIndex(indexer);
        HGRandomAccessResult result = index.scanValues();
        result.goBeforeFirst();
        int totalValues = 0;
        while (result.hasNext())
        {
 //           System.out.println(graph.get((HGHandle) result.next()));
            HGHandle value = (HGHandle)result.next();
            totalValues++;            
        }
        result.close();
        result = index.scanKeys();
        int totalValues2 = 0;
        while (result.hasNext())
        {
            long forkey = index.count(result.next());
            totalValues2 += forkey;
//            System.out.println("Count for " + result.current() + " --> " + forkey);
        }
        result.close();
        Assert.assertEquals(totalValues, totalValues2);
        for (int i = 0; i < 40; i++)
        {
            for (int j = 0; j < 25; j++)
            {
                HGPersistentHandle x = hg.findOne(graph, hg.eq(i * j));
                List<HGHandle> L = hg.findAll(graph, new IndexCondition(index,
                        x));
                List<Atom> AL = ramIndex.get(i * j);
                Assert.assertEquals(L.size(), AL.size());
                for (HGHandle h : L)
                    Assert.assertTrue(AL.contains(graph.get(h)));
            }
        }
    }
}
