package hgtest.storage.bje.KeyScanResultSet;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
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

    protected void putKeyValuePair(Cursor realCursor, final Integer key,
                                   final String value) {
        realCursor.put(
                new DatabaseEntry(new TestUtils.ByteArrayConverterForInteger()
                        .toByteArray(key)),
                new DatabaseEntry(new TestUtils.ByteArrayConverterForString()
                        .toByteArray(value)));
    }

    @Test
    public void thereIsOneKeyButItIsGreaterThanDesired() throws Exception {
        final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.close;

        startupCursor();
        putKeyValuePair(realCursor, 1, "one");
        startupMocks();

        final HGRandomAccessResult.GotoResult actual = keyScan.goTo(-5,
                EXACT_MATCH);

        assertEquals(actual, expected);
        shutdownCursor();
    }

    @Test
    public void thereIsOneKeyButItIsLessThanDesired() throws Exception {
        final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.nothing;

        startupCursor();
        putKeyValuePair(realCursor, 1, "one");
        startupMocks();

        final HGRandomAccessResult.GotoResult actual = keyScan.goTo(5,
                EXACT_MATCH);

        assertEquals(actual, expected);
        shutdownCursor();
    }

    @Test
    public void thereIsOneKeyAndItIsEqualToDesired() throws Exception {
        final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.found;

        startupCursor();
        putKeyValuePair(realCursor, 1, "one");
        startupMocks();

        final HGRandomAccessResult.GotoResult actual = keyScan.goTo(1,
                EXACT_MATCH);

        assertEquals(actual, expected);
        shutdownCursor();
    }

    @Test
    public void thereAreTwoKeysButAllOfThemAreGreaterToDesired() throws Exception {
        final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.close;

        startupCursor();
        putKeyValuePair(realCursor, 1, "one");
        putKeyValuePair(realCursor, 2, "two");
        startupMocks();

        final HGRandomAccessResult.GotoResult actual = keyScan.goTo(-5,
                EXACT_MATCH);

        assertEquals(actual, expected);
        shutdownCursor();
    }

    @Test
    public void thereAreTwoKeysButAllOfThemAreLessThanDesired() throws Exception {
        final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.nothing;

        startupCursor();
        putKeyValuePair(realCursor, 1, "one");
        putKeyValuePair(realCursor, 2, "two");
        startupMocks();

        final HGRandomAccessResult.GotoResult actual = keyScan.goTo(5,
                EXACT_MATCH);

        assertEquals(actual, expected);
        shutdownCursor();
    }

    @Test
    public void thereAreTwoKeysAndSomeOfThemAreEqualToDesired() throws Exception {
        final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.found;

        startupCursor();
        putKeyValuePair(realCursor, 1, "one");
        putKeyValuePair(realCursor, 2, "two");
        startupMocks();

        final HGRandomAccessResult.GotoResult actual = keyScan.goTo(2,
                EXACT_MATCH);

        assertEquals(actual, expected);
        shutdownCursor();
    }

    @Test
    public void thereAreTwoKeysAndAllOfThemAreEqualToDesired() throws Exception {
        final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.found;

        startupCursor();
        putKeyValuePair(realCursor, 1, "one");
        putKeyValuePair(realCursor, 1, "I");
        startupMocks();

        final HGRandomAccessResult.GotoResult actual = keyScan.goTo(1,
                EXACT_MATCH);

        assertEquals(actual, expected);
        shutdownCursor();
    }

    @Test
    public void thereAreSeveralKeysButAllOfThemAreGreaterToDesired() throws Exception {
        final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.close;

        startupCursor();
        putKeyValuePair(realCursor, 1, "one");
        putKeyValuePair(realCursor, 2, "two");
        putKeyValuePair(realCursor, 3, "three");
        startupMocks();

        final HGRandomAccessResult.GotoResult actual = keyScan.goTo(-5,
                EXACT_MATCH);

        assertEquals(actual, expected);
        shutdownCursor();
    }

    @Test
    public void thereAreSeveralKeysButAllOfThemAreLessThanDesired() throws Exception {
        final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.nothing;

        startupCursor();
        putKeyValuePair(realCursor, 1, "one");
        putKeyValuePair(realCursor, 2, "two");
        putKeyValuePair(realCursor, 3, "three");
        startupMocks();

        final HGRandomAccessResult.GotoResult actual = keyScan.goTo(5,
                EXACT_MATCH);

        assertEquals(actual, expected);
        shutdownCursor();
    }

    @Test
    public void thereAreSeveralKeysAndAllOfThemAreEqualToDesired() throws Exception {
        final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.found;

        startupCursor();
        putKeyValuePair(realCursor, 1, "one");
        putKeyValuePair(realCursor, 1, "I");
        putKeyValuePair(realCursor, 1, "first");
        startupMocks();

        final HGRandomAccessResult.GotoResult actual = keyScan.goTo(1,
                EXACT_MATCH);

        assertEquals(actual, expected);
        shutdownCursor();
    }

    @Test
    public void thereAreSeveralKeysAndSomeOfThemAreEqualToDesired() throws Exception {
        final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.found;

        startupCursor();
        putKeyValuePair(realCursor, 1, "one");
        putKeyValuePair(realCursor, 2, "two");
        putKeyValuePair(realCursor, 3, "three");
        startupMocks();

        final HGRandomAccessResult.GotoResult actual = keyScan.goTo(3,
                EXACT_MATCH);

        assertEquals(actual, expected);
        shutdownCursor();
    }
}
