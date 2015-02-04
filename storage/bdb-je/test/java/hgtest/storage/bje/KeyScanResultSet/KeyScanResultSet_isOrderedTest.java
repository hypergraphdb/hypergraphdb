package hgtest.storage.bje.KeyScanResultSet;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Transaction;
import hgtest.storage.bje.ResultSetTestBasis;
import hgtest.storage.bje.TestUtils;
import org.easymock.EasyMock;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.storage.bje.BJETxCursor;
import org.hypergraphdb.storage.bje.KeyScanResultSet;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;

/**
 * @author Yuriy Sechko
 */
public class KeyScanResultSet_isOrderedTest extends ResultSetTestBasis
{
	@Test
	public void test() throws Exception
	{
		final ByteArrayConverter<Integer> converter = new TestUtils.ByteArrayConverterForInteger();
		final Transaction transactionForTheRealCursor = environment
				.beginTransaction(null, null);
		final Cursor realCursor = database.openCursor(
				transactionForTheEnvironment, null);
		realCursor.put(new DatabaseEntry(new byte[] { 0, 1, 2, 3 }),
				new DatabaseEntry(new byte[] { 0, 1, 2, 3 }));
		final BJETxCursor fakeCursor = PowerMock.createMock(BJETxCursor.class);
		EasyMock.expect(fakeCursor.cursor()).andReturn(realCursor);
		PowerMock.replayAll();
		final KeyScanResultSet<Integer> keyScan = new KeyScanResultSet<Integer>(
				fakeCursor, null, converter);

		final boolean isOrdered = keyScan.isOrdered();

		assertTrue(isOrdered);
		realCursor.close();
		transactionForTheRealCursor.commit();

	}
}
