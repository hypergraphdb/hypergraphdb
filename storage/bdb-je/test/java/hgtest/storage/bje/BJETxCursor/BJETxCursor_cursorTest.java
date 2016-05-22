package hgtest.storage.bje.BJETxCursor;

import com.sleepycat.je.Cursor;
import org.hypergraphdb.storage.bje.BJETxCursor;
import org.hypergraphdb.storage.bje.TransactionBJEImpl;
import org.powermock.api.easymock.PowerMock;
import org.junit.Test;

import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class BJETxCursor_cursorTest extends BJETxCursorTestBasis
{
	@Test
	public void returnsNull_whenCursorIsNull() throws Exception
	{
		replay();

		final BJETxCursor bjeCursor = new BJETxCursor(null, mockedTx);
		final Cursor actual = bjeCursor.cursor();
		assertNull(actual);
	}

	@Test
	public void returnsTheSameInstance_whenCursorIsNotNull() throws Exception
	{
		final Cursor expected = mockedCursor;
		replay();

		final BJETxCursor bjeCursor = new BJETxCursor(mockedCursor, mockedTx);
		final Cursor actual = bjeCursor.cursor();
		assertSame(expected, actual);
	}
}
