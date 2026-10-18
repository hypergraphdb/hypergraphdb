package hgtest.query;

import static org.junit.Assert.*;

import java.util.NoSuchElementException;

import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.query.impl.InMemoryIntersectionResult;
import org.hypergraphdb.query.impl.IntersectionQuery;
import org.hypergraphdb.query.impl.KeyBasedQuery;
import org.hypergraphdb.query.impl.PipeQuery;
import org.hypergraphdb.query.impl.RSCombiner;
import org.hypergraphdb.query.impl.SortedIntersectionResult;
import org.hypergraphdb.query.impl.UnionQuery;
import org.hypergraphdb.query.impl.UnionResult;
import org.hypergraphdb.query.impl.ZigZagIntersectionResult;
import org.junit.Test;

/**
 * Compound queries and result sets must close every result set they opened, also when an
 * operation fails part way (typically a lock conflict surfacing in hasNext()). A result set
 * left open keeps its storage cursor, and the cursor's locks, until the transaction ends.
 */
public class ResultSetClosingTests
{
    static class Failure extends RuntimeException
    {
        private static final long serialVersionUID = 1L;
        Failure(String message) { super(message); }
    }

    /** A sorted, in-memory result set that counts close() calls and can fail on demand. */
    static class TrackingResult implements HGRandomAccessResult<Integer>
    {
        final int[] values;
        int position = -1;
        int closes = 0;
        boolean failOnHasNext = false;
        boolean failOnClose = false;

        TrackingResult(int... values) { this.values = values; }

        public void close()
        {
            closes++;
            if (failOnClose)
                throw new Failure("close failed");
        }
        public boolean hasNext()
        {
            if (failOnHasNext)
                throw new Failure("hasNext failed");
            return position + 1 < values.length;
        }
        public Integer next()
        {
            if (!hasNext())
                throw new NoSuchElementException();
            return values[++position];
        }
        public boolean hasPrev() { return position > 0; }
        public Integer prev()
        {
            if (!hasPrev())
                throw new NoSuchElementException();
            return values[--position];
        }
        public Integer current()
        {
            if (position < 0 || position >= values.length)
                throw new NoSuchElementException();
            return values[position];
        }
        public boolean isOrdered() { return true; }
        public void remove() { throw new UnsupportedOperationException(); }
        public void goBeforeFirst() { position = -1; }
        public void goAfterLast() { position = values.length; }
        public GotoResult goTo(Integer value, boolean exactMatch)
        {
            for (int i = 0; i < values.length; i++)
            {
                if (values[i] == value)
                {
                    position = i;
                    return GotoResult.found;
                }
                if (values[i] > value)
                {
                    if (exactMatch)
                        return GotoResult.nothing;
                    position = i;
                    return GotoResult.close;
                }
            }
            return GotoResult.nothing;
        }
    }

    static HGQuery<Integer> returning(final HGSearchResult<Integer> rs)
    {
        return new HGQuery<Integer>() {
            public HGSearchResult<Integer> execute() { return rs; }
        };
    }

    static HGQuery<Integer> failing()
    {
        return new HGQuery<Integer>() {
            public HGSearchResult<Integer> execute() { throw new Failure("execute failed"); }
        };
    }

    static final RSCombiner<Integer> ZIGZAG = new RSCombiner<Integer>() {
        public HGSearchResult<Integer> combine(HGSearchResult<Integer> l, HGSearchResult<Integer> r)
        {
            return new ZigZagIntersectionResult<Integer>((HGRandomAccessResult<Integer>)l, (HGRandomAccessResult<Integer>)r);
        }
    };

    static void expectFailure(Runnable action)
    {
        try
        {
            action.run();
            fail("expected a Failure");
        }
        catch (Failure expected)
        {
        }
    }

    // ------------------------------------------------------------------
    // IntersectionQuery
    // ------------------------------------------------------------------

    @Test
    public void intersectionClosesLeftWhenRightExecuteFails()
    {
        final TrackingResult left = new TrackingResult(1, 2, 3);
        expectFailure(() -> new IntersectionQuery<Integer>(returning(left), failing(), ZIGZAG).execute());
        assertEquals(1, left.closes);
    }

    @Test
    public void intersectionClosesBothWhenHasNextFails()
    {
        final TrackingResult left = new TrackingResult(1, 2, 3);
        final TrackingResult right = new TrackingResult(2, 3);
        left.failOnHasNext = true;
        expectFailure(() -> new IntersectionQuery<Integer>(returning(left), returning(right), ZIGZAG).execute());
        assertEquals(1, left.closes);
        assertEquals(1, right.closes);
    }

    @Test
    public void intersectionClosesBothWhenCombinerFails()
    {
        final TrackingResult left = new TrackingResult(1, 2, 3);
        final TrackingResult right = new TrackingResult(2, 3);
        RSCombiner<Integer> failingCombiner = new RSCombiner<Integer>() {
            public HGSearchResult<Integer> combine(HGSearchResult<Integer> l, HGSearchResult<Integer> r)
            {
                throw new Failure("combine failed");
            }
        };
        expectFailure(() -> new IntersectionQuery<Integer>(returning(left), returning(right), failingCombiner).execute());
        assertEquals(1, left.closes);
        assertEquals(1, right.closes);
    }

    @Test
    public void intersectionStillWorks()
    {
        HGSearchResult<Integer> rs = new IntersectionQuery<Integer>(returning(new TrackingResult(1, 2, 3, 5)),
                                                                   returning(new TrackingResult(2, 3, 4)), ZIGZAG).execute();
        try
        {
            assertEquals(Integer.valueOf(2), rs.next());
            assertEquals(Integer.valueOf(3), rs.next());
            assertFalse(rs.hasNext());
        }
        finally
        {
            rs.close();
        }
    }

    // ------------------------------------------------------------------
    // UnionQuery
    // ------------------------------------------------------------------

    @Test
    public void unionClosesLeftWhenRightExecuteFails()
    {
        final TrackingResult left = new TrackingResult(1, 2);
        expectFailure(() -> new UnionQuery<Integer>(returning(left), failing()).execute());
        assertEquals(1, left.closes);
    }

    @Test
    public void unionClosesBothWhenHasNextFails()
    {
        final TrackingResult left = new TrackingResult(1, 2);
        final TrackingResult right = new TrackingResult(3);
        right.failOnHasNext = true;
        expectFailure(() -> new UnionQuery<Integer>(returning(left), returning(right)).execute());
        assertEquals(1, left.closes);
        assertEquals(1, right.closes);
    }

    // ------------------------------------------------------------------
    // PipeQuery / PipedResult
    // ------------------------------------------------------------------

    @Test
    public void pipedResultClosesInputWhenPipeFails()
    {
        final TrackingResult in = new TrackingResult(1, 2);
        KeyBasedQuery<Integer, Integer> pipe = new KeyBasedQuery<Integer, Integer>() {
            Integer key;
            public void setKey(Integer key) { this.key = key; }
            public Integer getKey() { return key; }
            public HGSearchResult<Integer> execute() { throw new Failure("pipe failed"); }
        };
        expectFailure(() -> new PipeQuery<Integer, Integer>(returning(in), pipe).execute());
        assertEquals(1, in.closes);
    }

    // ------------------------------------------------------------------
    // close() of composite result sets
    // ------------------------------------------------------------------

    @Test
    public void unionResultClosesRightWhenLeftCloseFails()
    {
        TrackingResult left = new TrackingResult(1), right = new TrackingResult(2);
        left.failOnClose = true;
        final UnionResult rs = new UnionResult(left, right);
        expectFailure(() -> rs.close());
        assertEquals(1, right.closes);
    }

    @Test
    public void sortedIntersectionClosesRightWhenLeftCloseFails()
    {
        TrackingResult left = new TrackingResult(1), right = new TrackingResult(1);
        left.failOnClose = true;
        final SortedIntersectionResult<Integer> rs = new SortedIntersectionResult<Integer>(left, right);
        expectFailure(() -> rs.close());
        assertEquals(1, right.closes);
    }

    @Test
    public void zigZagClosesRightWhenLeftCloseFails()
    {
        TrackingResult left = new TrackingResult(1), right = new TrackingResult(1);
        left.failOnClose = true;
        final ZigZagIntersectionResult<Integer> rs = new ZigZagIntersectionResult<Integer>(left, right);
        expectFailure(() -> rs.close());
        assertEquals(1, right.closes);
    }

    // ------------------------------------------------------------------
    // InMemoryIntersectionResult
    // ------------------------------------------------------------------

    @Test
    public void inMemoryIntersectionClosesInputsWhenClosedUnread()
    {
        TrackingResult left = new TrackingResult(1, 2), right = new TrackingResult(2);
        new InMemoryIntersectionResult<Integer>(left, right).close();
        assertEquals(1, left.closes);
        assertEquals(1, right.closes);
    }

    @Test
    public void inMemoryIntersectionClosesInputsWhenIntersectingFails()
    {
        final TrackingResult left = new TrackingResult(1, 2), right = new TrackingResult(2);
        left.failOnHasNext = true;
        final InMemoryIntersectionResult<Integer> rs = new InMemoryIntersectionResult<Integer>(left, right);
        expectFailure(() -> rs.hasNext());
        rs.close();
        assertEquals(1, left.closes);
        assertEquals(1, right.closes);
    }

    @Test
    public void inMemoryIntersectionStillWorksAndClosesInputsOnce()
    {
        TrackingResult left = new TrackingResult(1, 2, 3), right = new TrackingResult(2, 3, 4);
        InMemoryIntersectionResult<Integer> rs = new InMemoryIntersectionResult<Integer>(left, right);
        assertEquals(Integer.valueOf(2), rs.next());
        assertEquals(Integer.valueOf(3), rs.next());
        assertFalse(rs.hasNext());
        rs.close();
        assertEquals(1, left.closes);
        assertEquals(1, right.closes);
    }
}
