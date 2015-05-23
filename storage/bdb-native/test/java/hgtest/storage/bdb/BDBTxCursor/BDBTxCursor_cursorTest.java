package hgtest.storage.bdb.BDBTxCursor;

import com.sleepycat.db.Cursor;
import org.hypergraphdb.storage.bdb.BDBTxCursor;
import org.hypergraphdb.storage.bdb.TransactionBDBImpl;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

/**
 * @author Yuriy Sechko
 */
public class BDBTxCursor_cursorTest
{
	@Test
	public void cursorIsNull() throws Exception
	{
		final Cursor cursor = null;
		final TransactionBDBImpl tx = PowerMock
				.createStrictMock(TransactionBDBImpl.class);
		PowerMock.replayAll();

		final BDBTxCursor bdbCursor = new BDBTxCursor(cursor, tx);
		final Cursor actual = bdbCursor.cursor();
		assertNull(actual);
		PowerMock.verifyAll();
	}

	@Test
	public void cursorIsNotNull() throws Exception
	{
		final Cursor expected = PowerMock.createStrictMock(Cursor.class);
		final TransactionBDBImpl tx = PowerMock
				.createStrictMock(TransactionBDBImpl.class);
		PowerMock.replayAll();

		final BDBTxCursor bdbCursor = new BDBTxCursor(expected, tx);
		final Cursor actual = bdbCursor.cursor();
		assertEquals(actual, expected);
		PowerMock.verifyAll();
	}
}
