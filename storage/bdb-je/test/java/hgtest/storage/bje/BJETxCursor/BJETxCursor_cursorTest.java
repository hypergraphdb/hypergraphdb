package hgtest.storage.bje.BJETxCursor;

import com.sleepycat.je.Cursor;
import org.hypergraphdb.storage.bje.BJETxCursor;
import org.hypergraphdb.storage.bje.TransactionBJEImpl;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

/**
 * @author Yuriy Sechko
 */
public class BJETxCursor_cursorTest
{
	@Test
	public void cursorIsNull() throws Exception
	{
		final Cursor cursor = null;
		final TransactionBJEImpl tx = PowerMock
				.createStrictMock(TransactionBJEImpl.class);
		PowerMock.replayAll();

		final BJETxCursor bjeCursor = new BJETxCursor(cursor, tx);
		final Cursor actual = bjeCursor.cursor();
		assertNull(actual);
		PowerMock.verifyAll();
	}

	@Test
	public void cursorIsNotNull() throws Exception
	{
		final Cursor expected = PowerMock.createStrictMock(Cursor.class);
		final TransactionBJEImpl tx = PowerMock
				.createStrictMock(TransactionBJEImpl.class);
		PowerMock.replayAll();

		final BJETxCursor bjeCursor = new BJETxCursor(expected, tx);
		final Cursor actual = bjeCursor.cursor();
		assertEquals(actual, expected);
		PowerMock.verifyAll();
	}
}
