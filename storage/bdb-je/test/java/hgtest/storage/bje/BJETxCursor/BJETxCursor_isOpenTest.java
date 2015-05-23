package hgtest.storage.bje.BJETxCursor;

import com.sleepycat.je.Cursor;
import org.hypergraphdb.storage.bje.BJETxCursor;
import org.hypergraphdb.storage.bje.TransactionBJEImpl;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * @author Yuriy Sechko
 */
public class BJETxCursor_isOpenTest
{
	@Test
	public void cursorIsNull() throws Exception
	{
		final Cursor cursor = null;
		final TransactionBJEImpl tx = PowerMock
				.createMock(TransactionBJEImpl.class);
		PowerMock.replayAll();

		final BJETxCursor bjeCursor = new BJETxCursor(cursor, tx);
		final boolean actual = bjeCursor.isOpen();
		assertFalse(actual);
		PowerMock.verifyAll();
	}

	@Test
	public void cursorIsNotNull() throws Exception
	{
		final Cursor cursor = PowerMock.createStrictMock(Cursor.class);
		final TransactionBJEImpl tx = PowerMock
				.createStrictMock(TransactionBJEImpl.class);
		PowerMock.replayAll();

		final BJETxCursor bjeCursor = new BJETxCursor(cursor, tx);
		final boolean actual = bjeCursor.isOpen();
		assertTrue(actual);
		PowerMock.verifyAll();
	}
}
