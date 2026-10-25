package hgtest.query;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.query.impl.SortedIntersectionResult;
import org.junit.Test;

import hgtest.query.ResultSetClosingTests.TrackingResult;

/**
 * SortedIntersectionResult must produce the same elements walking backwards (prev) as forwards (next).
 */
public class SortedIntersectionTests
{
    static List<Integer> forward(HGSearchResult<Integer> rs)
    {
        List<Integer> L = new ArrayList<Integer>();
        while (rs.hasNext())
            L.add(rs.next());
        return L;
    }

    static List<Integer> backwardFromCurrent(HGSearchResult<Integer> rs)
    {
        List<Integer> L = new ArrayList<Integer>();
        L.add(rs.current());
        while (rs.hasPrev())
            L.add(rs.prev());
        return L;
    }

    static void checkBothDirections(int[] left, int[] right, Integer... expected)
    {
        SortedIntersectionResult<Integer> rs = new SortedIntersectionResult<Integer>(
            new TrackingResult(left), new TrackingResult(right));
        try
        {
            List<Integer> fwd = forward(rs);
            assertEquals("forwards", Arrays.asList(expected), fwd);
            List<Integer> reversed = new ArrayList<Integer>(fwd);
            Collections.reverse(reversed);
            assertEquals("backwards", reversed, backwardFromCurrent(rs));
        }
        finally
        {
            rs.close();
        }
    }

    @Test
    public void leftAheadOfRightWalkingBack()
    {
        // Walking back from 9, the left side's previous element (8) is greater than the right's (5):
        // the left side must step back.
        checkBothDirections(new int[] { 1, 3, 5, 8, 9 }, new int[] { 3, 5, 9 }, 3, 5, 9);
    }

    @Test
    public void rightAheadOfLeftWalkingBack()
    {
        checkBothDirections(new int[] { 3, 5, 9 }, new int[] { 1, 3, 5, 8, 9 }, 3, 5, 9);
    }

    @Test
    public void interleavedInputs()
    {
        checkBothDirections(new int[] { 1, 2, 4, 6, 7, 10, 12, 15 },
                            new int[] { 2, 3, 6, 8, 9, 10, 11, 15, 16 },
                            2, 6, 10, 15);
    }
}
