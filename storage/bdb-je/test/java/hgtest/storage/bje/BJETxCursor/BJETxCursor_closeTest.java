package hgtest.storage.bje.BJETxCursor;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseException;
import org.easymock.EasyMock;
import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.bje.BJETxCursor;
import org.hypergraphdb.storage.bje.TransactionBJEImpl;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

import static hgtest.storage.bje.TestUtils.assertExceptions;


/**
 * @author Yuriy Sechko
 */
public class BJETxCursor_closeTest
{
	@Test
	public void cursorIsNotOpened() throws Exception
	{
		final Cursor cursor = null;
		final TransactionBJEImpl tx = PowerMock
				.createStrictMock(TransactionBJEImpl.class);
		PowerMock.replayAll();

		final BJETxCursor bjeCursor = new BJETxCursor(cursor, tx);
		bjeCursor.close();

		PowerMock.verifyAll();
	}

	@Test
	public void cursorIsOpened() throws Exception
	{
		final Cursor cursor = PowerMock.createStrictMock(Cursor.class);
		cursor.close();
		final TransactionBJEImpl tx = PowerMock
				.createMock(TransactionBJEImpl.class);
		final BJETxCursor bjeCursor = new BJETxCursor(cursor, tx);
		recordRemoveCursor(tx, bjeCursor);
		PowerMock.replayAll();

		bjeCursor.close();

		PowerMock.verifyAll();
	}

	@Test
	public void txIsNull() throws Exception
	{
		final Cursor cursor = PowerMock.createStrictMock(Cursor.class);
		cursor.close();
		final TransactionBJEImpl tx = null;
		PowerMock.replayAll();

		final BJETxCursor bjeCursor = new BJETxCursor(cursor, tx);
		bjeCursor.close();

		PowerMock.verifyAll();
	}

	@Test
	public void innerCursorThrowsDatabaseException() throws Exception
	{
		final Cursor cursor = PowerMock.createStrictMock(Cursor.class);
		cursor.close();
		// when BJETxCursor.cursor throws DatabaseException then HGException is
		// rethrown in BJETxCursor.close() method
		EasyMock.expectLastCall()
				.andThrow(
						new CustomDatabaseException(
								"This is custom DatabaseException"));
		final TransactionBJEImpl tx = PowerMock
				.createMock(TransactionBJEImpl.class);
		final BJETxCursor bjeCursor = new BJETxCursor(cursor, tx);
		recordRemoveCursor(tx, bjeCursor);
		PowerMock.replayAll();

		try
		{
			bjeCursor.close();
		}
		catch (Exception ex)
		{
			assertExceptions(
					ex,
					HGException.class,
					"hgtest.storage.bje.BJETxCursor.BJETxCursor_closeTest$CustomDatabaseException",
					"This is custom DatabaseException");
		}
		finally
		{
			PowerMock.verifyAll();
		}
	}

	@Test
	public void innerCursorThrowsOtherException() throws Exception
	{
		final Exception expected = new IllegalStateException();

		final Cursor cursor = PowerMock.createStrictMock(Cursor.class);
		cursor.close();
		// when BJETxCursor.cursor throws something differ from
		// DatabaseException then nothing is caught and rethrown in
		// BJETxCursor.close()
		// method
		EasyMock.expectLastCall().andThrow(new IllegalStateException());
		final TransactionBJEImpl tx = PowerMock
				.createMock(TransactionBJEImpl.class);
		final BJETxCursor bjeCursor = new BJETxCursor(cursor, tx);
		recordRemoveCursor(tx, bjeCursor);
		PowerMock.replayAll();

		try
		{
			bjeCursor.close();
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
		}
		finally
		{
			PowerMock.verifyAll();
		}
	}

	/**
	 * Inside {@link org.hypergraphdb.storage.bje.BJETxCursor#close()} method
	 * {@link org.hypergraphdb.storage.bje.TransactionBJEImpl#removeCursor(org.hypergraphdb.storage.bje.BJETxCursor)}
	 * is called.
	 * {@link org.hypergraphdb.storage.bje.TransactionBJEImpl#removeCursor(org.hypergraphdb.storage.bje.BJETxCursor)}
	 * is private method. It cannot be recorded as public method. So we need
	 * invoke it by name manually.
	 */
	private void recordRemoveCursor(final TransactionBJEImpl tx,
			final BJETxCursor bjeCursor) throws Exception
	{
		final Method removeCursorMethod = tx.getClass().getDeclaredMethod(
				"removeCursor", BJETxCursor.class);
		removeCursorMethod.setAccessible(true);
		removeCursorMethod.invoke(tx, bjeCursor);
	}

	/**
	 * Just a subclass for DatabaseException (which is abstract)
	 */
	class CustomDatabaseException extends DatabaseException
	{
		public CustomDatabaseException(final String message)
		{
			super(message);
		}
	}
}
