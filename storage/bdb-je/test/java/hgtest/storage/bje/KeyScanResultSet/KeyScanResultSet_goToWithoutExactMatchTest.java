package hgtest.storage.bje.KeyScanResultSet;

import hgtest.storage.bje.TestUtils;
import org.hypergraphdb.HGRandomAccessResult;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @author Yuriy Sechko
 */
public class KeyScanResultSet_goToWithoutExactMatchTest extends
        KeyScanResultSet_goToTestBasis {
    private static final boolean EXACT_MATCH = false;

    @Test
    public void thereIsOneKeyButItIsGreaterThanDesired() throws Exception {
        final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.close;

        startupCursor();
        TestUtils.putKeyValuePair(realCursor, 1, "one");
        createMocksForTheGoTo();

        final HGRandomAccessResult.GotoResult actual = keyScan.goTo(-5,
                EXACT_MATCH);

        assertEquals(actual, expected);
        shutdownCursor();
    }

    @Test
    public void thereIsOneKeyButItIsLessThanDesired() throws Exception {
        final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.nothing;

        startupCursor();
        TestUtils.putKeyValuePair(realCursor, 1, "one");
        createMocksForTheGoTo();

        final HGRandomAccessResult.GotoResult actual = keyScan.goTo(5,
                EXACT_MATCH);

        assertEquals(actual, expected);
        shutdownCursor();
    }

    @Test
    public void thereIsOneKeyAndItIsEqualToDesired() throws Exception {
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
    public void thereAreTwoKeysButAllOfThemAreGreaterToDesired() throws Exception {
        final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.close;

        startupCursor();
        TestUtils.putKeyValuePair(realCursor, 1, "one");
        TestUtils.putKeyValuePair(realCursor, 2, "two");
        createMocksForTheGoTo();

        final HGRandomAccessResult.GotoResult actual = keyScan.goTo(-5,
                EXACT_MATCH);

        assertEquals(actual, expected);
        shutdownCursor();
    }

    @Test
    public void thereAreTwoKeysButAllOfThemAreLessThanDesired() throws Exception {
        final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.nothing;

        startupCursor();
        TestUtils.putKeyValuePair(realCursor, 1, "one");
        TestUtils.putKeyValuePair(realCursor, 2, "two");
        createMocksForTheGoTo();

        final HGRandomAccessResult.GotoResult actual = keyScan.goTo(5,
                EXACT_MATCH);

        assertEquals(actual, expected);
        shutdownCursor();
    }

    @Test
    public void thereAreTwoKeysAndSomeOfThemAreEqualToDesired() throws Exception {
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
    public void thereAreTwoKeysAndAllOfThemAreEqualToDesired() throws Exception {
        final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.found;

        startupCursor();
        TestUtils.putKeyValuePair(realCursor, 1, "one");
        TestUtils.putKeyValuePair(realCursor, 1, "I");
        createMocksForTheGoTo();

        final HGRandomAccessResult.GotoResult actual = keyScan.goTo(1,
                EXACT_MATCH);

        assertEquals(actual, expected);
        shutdownCursor();
    }

    @Test
    public void thereAreSeveralKeysButAllOfThemAreGreaterToDesired() throws Exception {
        final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.close;

        startupCursor();
        TestUtils.putKeyValuePair(realCursor, 1, "one");
        TestUtils.putKeyValuePair(realCursor, 2, "two");
        TestUtils.putKeyValuePair(realCursor, 3, "three");
        createMocksForTheGoTo();

        final HGRandomAccessResult.GotoResult actual = keyScan.goTo(-5,
                EXACT_MATCH);

        assertEquals(actual, expected);
        shutdownCursor();
    }

    @Test
    public void thereAreSeveralKeysButAllOfThemAreLessThanDesired() throws Exception {
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
    public void thereAreSeveralKeysAndAllOfThemAreEqualToDesired() throws Exception {
        final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.found;

        startupCursor();
        TestUtils.putKeyValuePair(realCursor, 1, "one");
        TestUtils.putKeyValuePair(realCursor, 1, "I");
        TestUtils.putKeyValuePair(realCursor, 1, "first");
        createMocksForTheGoTo();

        final HGRandomAccessResult.GotoResult actual = keyScan.goTo(1,
                EXACT_MATCH);

        assertEquals(actual, expected);
        shutdownCursor();
    }

    @Test
    public void thereAreSeveralKeysAndSomeOfThemAreEqualToDesired() throws Exception {
        final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.found;

        startupCursor();
        TestUtils.putKeyValuePair(realCursor, 1, "one");
        TestUtils.putKeyValuePair(realCursor, 2, "two");
        TestUtils.putKeyValuePair(realCursor, 3, "three");
        createMocksForTheGoTo();

        final HGRandomAccessResult.GotoResult actual = keyScan.goTo(3,
                EXACT_MATCH);

        assertEquals(actual, expected);
        shutdownCursor();
    }
}
