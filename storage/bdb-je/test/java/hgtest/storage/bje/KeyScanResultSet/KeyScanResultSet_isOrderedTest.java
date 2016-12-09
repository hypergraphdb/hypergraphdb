package hgtest.storage.bje.KeyScanResultSet;

import static hgtest.storage.bje.TestUtils.putKeyValuePair;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.easymock.PowerMock.replayAll;

import org.hypergraphdb.storage.bje.KeyScanResultSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KeyScanResultSet_isOrderedTest extends KeyScanResultSetTestBasis
{
	@Test
	public void test() throws Exception
	{
		putKeyValuePair(realCursor, 0, "stub");
		createMocksForTheConstructor();
		replayAll();

		final KeyScanResultSet<Integer> keyScan = new KeyScanResultSet<>(
				fakeCursor, null, converter);

		final boolean isOrdered = keyScan.isOrdered();

		assertTrue(isOrdered);
	}

	@Before
	public void startup()
	{
		startupCursor();
	}

	@After
	public void shutdown()
	{
		shutdownCursor();
	}
}
