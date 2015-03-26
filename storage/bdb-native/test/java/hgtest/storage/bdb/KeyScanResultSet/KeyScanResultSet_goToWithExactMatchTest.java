package hgtest.storage.bdb.KeyScanResultSet;

import hgtest.storage.bdb.TestUtils;
import org.hypergraphdb.HGRandomAccessResult;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @author Yuriy Sechko
 */
public class KeyScanResultSet_goToWithExactMatchTest extends
        KeyScanResultSet_goToTestBasis
{
    private static final boolean EXACT_MATCH = true;

    @Test
    public void thereIsOneKeyButItIsNotEqualToDesired() throws Exception
    {
        final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.nothing;

        startupCursor();
        TestUtils.putKeyValuePair(realCursor, 1, "one");
        createMocksForTheGoTo();

        final HGRandomAccessResult.GotoResult actual = keyScan.goTo(2,
                EXACT_MATCH);

        assertEquals(actual, expected);
        shutdownCursor();
    }

    @Test
    public void thereOneKeyButItIsEqualToDesired() throws Exception
    {
        final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.found;

        startupCursor();
        TestUtils.putKeyValuePair(realCursor, 1, "one");
        createMocksForTheGoTo();

        final HGRandomAccessResult.GotoResult actual = keyScan.goTo(1,
                EXACT_MATCH);

        assertEquals(actual, expected);
        shutdownCursor();
    }

    @Test
    public void thereOneKeyButItIsCloseToDesired() throws Exception
    {
        final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.nothing;

        startupCursor();
        TestUtils.putKeyValuePair(realCursor, 1, "one");
        createMocksForTheGoTo();

        final HGRandomAccessResult.GotoResult actual = keyScan.goTo(2,
                EXACT_MATCH);

        assertEquals(actual, expected);
        shutdownCursor();
    }

    @Test
    public void thereAreTwoItemsButThereIsNotDesired() throws Exception
    {
        final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.nothing;

        startupCursor();
        TestUtils.putKeyValuePair(realCursor, 1, "one");
        TestUtils.putKeyValuePair(realCursor, 2, "two");
        createMocksForTheGoTo();

        final HGRandomAccessResult.GotoResult actual = keyScan.goTo(-2,
                EXACT_MATCH);

        assertEquals(actual, expected);
        shutdownCursor();
    }

    @Test
    public void thereAreTwoItemsAndOnOfThemIsCloseToDesired() throws Exception
    {
        final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.nothing;

        startupCursor();
        TestUtils.putKeyValuePair(realCursor, 1, "one");
        TestUtils.putKeyValuePair(realCursor, 3, "three");
        createMocksForTheGoTo();

        final HGRandomAccessResult.GotoResult actual = keyScan.goTo(2,
                EXACT_MATCH);

        assertEquals(actual, expected);
        shutdownCursor();
    }

    @Test
    public void thereAreTwoItemsAndOnOfThemIsEqualToDesired() throws Exception
    {
        final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.found;

        startupCursor();
        TestUtils.putKeyValuePair(realCursor, 1, "one");
        TestUtils.putKeyValuePair(realCursor, 2, "two");
        createMocksForTheGoTo();

        final HGRandomAccessResult.GotoResult actual = keyScan.goTo(2,
                EXACT_MATCH);

        assertEquals(actual, expected);
        shutdownCursor();
    }

    @Test
    public void thereAreThreeItemsButThereIsNotDesired() throws Exception
    {
        final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.nothing;

        startupCursor();
        TestUtils.putKeyValuePair(realCursor, 1, "one");
        TestUtils.putKeyValuePair(realCursor, 2, "two");
        TestUtils.putKeyValuePair(realCursor, 3, "three");
        createMocksForTheGoTo();

        final HGRandomAccessResult.GotoResult actual = keyScan.goTo(5,
                EXACT_MATCH);

        assertEquals(actual, expected);
        shutdownCursor();
    }

    @Test
    public void thereAreThreeItemsAndOneOfThemIsEqualToDesired()
            throws Exception
    {
        final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.found;

        startupCursor();
        TestUtils.putKeyValuePair(realCursor, 1, "one");
        TestUtils.putKeyValuePair(realCursor, 2, "two");
        TestUtils.putKeyValuePair(realCursor, 3, "three");
        createMocksForTheGoTo();

        final HGRandomAccessResult.GotoResult actual = keyScan.goTo(2,
                EXACT_MATCH);

        assertEquals(actual, expected);
        shutdownCursor();
    }

    @Test
    public void thereAreThreeItemsAndSeveralOfThemAreEqualToDesired()
            throws Exception
    {
        final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.found;

        startupCursor();
        TestUtils.putKeyValuePair(realCursor, 1, "one");
        TestUtils.putKeyValuePair(realCursor, 2, "two");
        TestUtils.putKeyValuePair(realCursor, 3, "three");
        TestUtils.putKeyValuePair(realCursor, 3, "III");
        createMocksForTheGoTo();

        final HGRandomAccessResult.GotoResult actual = keyScan.goTo(3,
                EXACT_MATCH);
        assertEquals(actual, expected);
        shutdownCursor();
    }
}

