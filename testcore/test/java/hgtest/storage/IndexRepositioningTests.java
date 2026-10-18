package hgtest.storage;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

import org.hypergraphdb.HGBidirectionalIndex;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGIndex;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGPlainLink;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HGSortIndex;
import org.hypergraphdb.storage.BAtoString;
import org.hypergraphdb.storage.StorageBasedIncidenceSet;
import org.junit.BeforeClass;
import org.junit.Test;

import hgtest.HGTestBase;

/**
 * Index result sets moved to before their first or after their last element (goBeforeFirst,
 * goAfterLast) must stay within the elements of the result set: the values of one key, the keys
 * having one value, or a key range. They used to move to the first or last record of the whole
 * index database instead.
 */
public class IndexRepositioningTests extends HGTestBase
{
    static final List<String> KEYS = Arrays.asList("a", "b", "c", "d");
    static HGSortIndex<String, String> index;
    static HGBidirectionalIndex<String, String> biIndex;

    @SuppressWarnings("unchecked")
    @BeforeClass
    public static void setUp()
    {
        HGTestBase.setUp();
        final HGIndex<String, String> plain = graph.getStore().getIndex("test_repositioning",
            BAtoString.getInstance(), BAtoString.getInstance(), null, null, true);
        index = (HGSortIndex<String, String>)plain;
        biIndex = graph.getStore().getBidirectionalIndex("test_repositioning_bi",
            BAtoString.getInstance(), BAtoString.getInstance(), null, null, true);
        graph.getTransactionManager().transact(new Callable<Object>() {
            public Object call()
            {
                for (String k : KEYS)
                    for (int i = 1; i <= 3; i++)
                        index.addEntry(k, k + i);
                // value -> keys: "v" has k1..k3, "w" has k4, "z" (the last value) has k5..k6
                biIndex.addEntry("k1", "v");
                biIndex.addEntry("k2", "v");
                biIndex.addEntry("k3", "v");
                biIndex.addEntry("k4", "w");
                biIndex.addEntry("k5", "z");
                biIndex.addEntry("k6", "z");
                return null;
            }
        });
    }

    static <T> T inTx(Callable<T> work)
    {
        return graph.getTransactionManager().ensureTransaction(work);
    }

    static <T> List<T> forward(HGSearchResult<T> rs)
    {
        List<T> L = new ArrayList<T>();
        while (rs.hasNext())
            L.add(rs.next());
        return L;
    }

    static <T> List<T> backward(HGSearchResult<T> rs)
    {
        List<T> L = new ArrayList<T>();
        while (rs.hasPrev())
            L.add(rs.prev());
        return L;
    }

    static List<String> list(String... values)
    {
        return Arrays.asList(values);
    }

    /** All elements in order, then after-last, before-first, and the elements again in both directions. */
    static void checkRepositioning(final Callable<HGRandomAccessResult<String>> open, final List<String> expected)
    {
        inTx(() -> {
            HGRandomAccessResult<String> rs = open.call();
            try
            {
                assertEquals(expected, forward(rs));
                rs.goBeforeFirst();
                assertFalse("nothing before the first element", rs.hasPrev());
                assertEquals("first element after goBeforeFirst", expected.get(0), rs.next());
                rs.goBeforeFirst();
                assertEquals("all elements after goBeforeFirst", expected, forward(rs));
                rs.goAfterLast();
                assertFalse("nothing after the last element", rs.hasNext());
                assertEquals("last element after goAfterLast", expected.get(expected.size() - 1), rs.prev());
                rs.goAfterLast();
                List<String> reversed = new ArrayList<String>(expected);
                Collections.reverse(reversed);
                assertEquals("all elements backwards after goAfterLast", reversed, backward(rs));
            }
            finally
            {
                rs.close();
            }
            return null;
        });
    }

    // ------------------------------------------------------------------
    // Values of one key (SingleKeyResultSet)
    // ------------------------------------------------------------------

    @Test
    public void singleKeyInTheMiddle()
    {
        checkRepositioning(() -> (HGRandomAccessResult<String>)index.find("b"), list("b1", "b2", "b3"));
    }

    @Test
    public void singleKeyFirstInTheDatabase()
    {
        checkRepositioning(() -> (HGRandomAccessResult<String>)index.find("a"), list("a1", "a2", "a3"));
    }

    @Test
    public void singleKeyLastInTheDatabase()
    {
        checkRepositioning(() -> (HGRandomAccessResult<String>)index.find("d"), list("d1", "d2", "d3"));
    }

    // ------------------------------------------------------------------
    // Keys having one value (SingleValueResultSet)
    // ------------------------------------------------------------------

    @Test
    public void singleValueInTheMiddle()
    {
        checkRepositioning(() -> biIndex.findByValue("v"), list("k1", "k2", "k3"));
    }

    @Test
    public void singleValueLastInTheDatabase()
    {
        checkRepositioning(() -> biIndex.findByValue("z"), list("k5", "k6"));
    }

    // ------------------------------------------------------------------
    // Key ranges (KeyRangeForwardResultSet, KeyRangeBackwardResultSet)
    // ------------------------------------------------------------------

    @Test
    public void forwardRangeFromInclusiveKey()
    {
        checkRepositioning(() -> (HGRandomAccessResult<String>)index.findGTE("b"),
                           list("b1", "b2", "b3", "c1", "c2", "c3", "d1", "d2", "d3"));
    }

    @Test
    public void forwardRangeFromExclusiveKey()
    {
        checkRepositioning(() -> (HGRandomAccessResult<String>)index.findGT("b"),
                           list("c1", "c2", "c3", "d1", "d2", "d3"));
    }

    @Test
    public void backwardRangeToInclusiveKey()
    {
        checkRepositioning(() -> (HGRandomAccessResult<String>)index.findLTE("c"),
                           list("c3", "c2", "c1", "b3", "b2", "b1", "a3", "a2", "a1"));
    }

    @Test
    public void backwardRangeToExclusiveKey()
    {
        checkRepositioning(() -> (HGRandomAccessResult<String>)index.findLT("c"),
                           list("b3", "b2", "b1", "a3", "a2", "a1"));
    }

    /** Moving back over the values of the range's first key must work (it used to stop at that key). */
    @Test
    public void rangesMoveBackWithinTheirFirstKey()
    {
        inTx(() -> {
            HGRandomAccessResult<String> rs = (HGRandomAccessResult<String>)index.findGTE("b");
            try
            {
                assertEquals("b1", rs.next());
                assertEquals("b2", rs.next());
                assertEquals("b3", rs.next());
                assertTrue(rs.hasPrev());
                assertEquals("b2", rs.prev());
                assertEquals("b1", rs.prev());
                assertFalse("nothing before the range", rs.hasPrev());
            }
            finally
            {
                rs.close();
            }
            rs = (HGRandomAccessResult<String>)index.findLTE("c");
            try
            {
                assertEquals("c3", rs.next());
                assertEquals("c2", rs.next());
                assertTrue(rs.hasPrev());
                assertEquals("c3", rs.prev());
                assertFalse("nothing before the range", rs.hasPrev());
            }
            finally
            {
                rs.close();
            }
            return null;
        });
    }

    // ------------------------------------------------------------------
    // Large incidence sets (StorageBasedIncidenceSet)
    // ------------------------------------------------------------------

    @Test
    public void storageBasedIncidenceSetLast()
    {
        // Several atoms with incident links, so that the incidence index has keys before and after the one tested.
        final List<HGPersistentHandle> atoms = new ArrayList<HGPersistentHandle>();
        for (int i = 0; i < 6; i++)
        {
            HGHandle atom = graph.add("repositioning atom " + i);
            atoms.add(atom.getPersistent());
            for (int j = 0; j < 3; j++)
                graph.add(new HGPlainLink(atom));
        }
        List<HGPersistentHandle> sorted = new ArrayList<HGPersistentHandle>(atoms);
        Collections.sort(sorted);
        for (HGPersistentHandle atom : Arrays.asList(sorted.get(0), sorted.get(2), sorted.get(sorted.size() - 1)))
        {
            List<HGPersistentHandle> incident = new ArrayList<HGPersistentHandle>();
            for (HGHandle h : graph.getIncidenceSet(atom))
                incident.add(h.getPersistent());
            Collections.sort(incident);
            HGHandle last = inTx(() -> new StorageBasedIncidenceSet(atom, graph).last());
            assertEquals("last() of the incidence set of " + atom, incident.get(incident.size() - 1), last.getPersistent());
        }
    }
}
