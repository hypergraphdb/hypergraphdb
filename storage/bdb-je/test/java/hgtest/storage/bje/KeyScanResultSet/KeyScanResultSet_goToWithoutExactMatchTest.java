package hgtest.storage.bje.KeyScanResultSet;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Transaction;
import hgtest.storage.bje.TestUtils;
import org.easymock.EasyMock;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.storage.bje.BJETxCursor;
import org.hypergraphdb.storage.bje.KeyScanResultSet;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @author Yuriy Sechko
 */
public class KeyScanResultSet_goToWithoutExactMatchTest extends
		KeyScanResultSet_goToTestBasis
{
	private static final boolean EXACT_MATCH = false;

	protected void putKeyValuePair(Cursor realCursor, final Integer key,
			final String value)
	{
		realCursor.put(
				new DatabaseEntry(new TestUtils.ByteArrayConverterForInteger()
						.toByteArray(key)),
				new DatabaseEntry(new TestUtils.ByteArrayConverterForString()
						.toByteArray(value)));
	}

	@Test
	public void thereIsOneKeyButItIsGreaterThanDesired() throws Exception
	{
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
    public void thereIsOneKeyButItIsLessThanDesired() throws Exception
    {
        final HGRandomAccessResult.GotoResult expected = HGRandomAccessResult.GotoResult.nothing;

        startupCursor();
        putKeyValuePair(realCursor, 1, "one");
        startupMocks();

        final HGRandomAccessResult.GotoResult actual = keyScan.goTo(5,
                EXACT_MATCH);

        assertEquals(actual, expected);
        shutdownCursor();
    }

}
