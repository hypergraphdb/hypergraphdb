package hgtest.storage.bje.KeyScanResultSet;

import hgtest.storage.bje.TestUtils;
import org.hypergraphdb.storage.bje.KeyScanResultSet;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;

/**
 * @author Yuriy Sechko
 */
public class KeyScanResultSet_isOrderedTest extends KeyScanResultSetTestBasis
{
	@Test
	public void test() throws Exception
	{
		startupCursor();
		TestUtils.putKeyValuePair(realCursor, 0, "stub");
		createMocksForTheConstructor();
		PowerMock.replayAll();
		final KeyScanResultSet<Integer> keyScan = new KeyScanResultSet<Integer>(
				fakeCursor, null, converter);

		final boolean isOrdered = keyScan.isOrdered();

		assertTrue(isOrdered);
		shutdownCursor();
	}
}
