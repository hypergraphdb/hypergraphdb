package hgtest.storage.bdb.BDBTxCursor;

import com.sleepycat.db.Cursor;
import org.hypergraphdb.storage.bdb.BDBTxCursor;
import org.hypergraphdb.storage.bdb.TransactionBDBImpl;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * @author Yuriy Sechko
 */
public class BDBTxCursor_isOpenTest
{
	@Test
	public void cursorIsNull() throws Exception
	{
		final Cursor cursor = null;
		final TransactionBDBImpl tx = PowerMock
				.createMock(TransactionBDBImpl.class);
		PowerMock.replayAll();

		final BDBTxCursor bdbCursor = new BDBTxCursor(cursor, tx);
		final boolean actual = bdbCursor.isOpen();
		assertFalse(actual);
		PowerMock.verifyAll();
	}

	@Test
	public void cursorIsNotNull() throws Exception
	{
		final Cursor cursor = PowerMock.createStrictMock(Cursor.class);
		final TransactionBDBImpl tx = PowerMock
				.createStrictMock(TransactionBDBImpl.class);
		PowerMock.replayAll();

		final BDBTxCursor bdbCursor = new BDBTxCursor(cursor, tx);
		final boolean actual = bdbCursor.isOpen();
		assertTrue(actual);
		PowerMock.verifyAll();
	}
}
