package hgtest.storage.bje.BJETxCursor;

import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.hamcrest.core.AllOf.allOf;
import static org.hamcrest.core.StringContains.containsString;

import java.lang.reflect.Method;

import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.bje.BJETxCursor;
import org.hypergraphdb.storage.bje.TransactionBJEImpl;
import org.junit.Test;

import com.sleepycat.je.DatabaseException;

public class BJETxCursor_closeTest extends  BJETxCursorTestBasis
{
	@Test
	public void doesNotFail_whenCursorIsNotOpenedAhead() throws Exception
	{
		replay();

		final BJETxCursor bjeCursor = new BJETxCursor(null, mockedTx);
		bjeCursor.close();
	}

	@Test
	public void happyPath() throws Exception
	{
		mockedCursor.close();
		final BJETxCursor bjeCursor = new BJETxCursor(mockedCursor, mockedTx);
		recordRemoveCursor(mockedTx, bjeCursor);
		replay();

		bjeCursor.close();
	}

	@Test
	public void doesNotFail_whenTxIsNull() throws Exception
	{
		mockedCursor.close();
		replay();

		final BJETxCursor bjeCursor = new BJETxCursor(mockedCursor, null);
		bjeCursor.close();
	}

	@Test
	public void wrapsDatabaseException_whenHypergraphException()
			throws Exception
	{
		mockedCursor.close();
		expectLastCall().andThrow(
				new DummyDatabaseException("This is custom DatabaseException"));
		final BJETxCursor bjeCursor = new BJETxCursor(mockedCursor, mockedTx);
		recordRemoveCursor(mockedTx, bjeCursor);
		replay();

		below.expect(HGException.class);
		below.expectMessage(allOf(
				containsString("hgtest.storage.bje.BJETxCursor.BJETxCursor_closeTest$DummyDatabaseException"),
				containsString("This is custom DatabaseException")));
		bjeCursor.close();
	}

	@Test
	public void doesNotWrapsOtherExceptions() throws Exception
	{
		mockedCursor.close();
		// when BJETxCursor.cursor throws something differ from
		// DatabaseException then nothing is caught and rethrown in
		// BJETxCursor.close() method
		expectLastCall().andThrow(new IllegalStateException());
		final BJETxCursor bjeCursor = new BJETxCursor(mockedCursor, mockedTx);
		recordRemoveCursor(mockedTx, bjeCursor);
		replay();

		below.expect(IllegalStateException.class);
		bjeCursor.close();
	}



	/**
	 * Inside {@link org.hypergraphdb.storage.bje.BJETxCursor#close()} method
	 * {@link org.hypergraphdb.storage.bje.TransactionBJEImpl#removeCursor(org.hypergraphdb.storage.bje.BJETxCursor)}
	 * is called.
	 * {@link org.hypergraphdb.storage.bje.TransactionBJEImpl#removeCursor(org.hypergraphdb.storage.bje.BJETxCursor)}
	 * is private method. It cannot be recorded as public method on mocked
	 * object. So we need invoke it by name manually.
	 */
	private static void recordRemoveCursor(final TransactionBJEImpl tx,
			final BJETxCursor bjeCursor) throws Exception
	{
		final Method removeCursorMethod = tx.getClass().getDeclaredMethod(
				"removeCursor", BJETxCursor.class);
		removeCursorMethod.setAccessible(true);
		removeCursorMethod.invoke(tx, bjeCursor);
	}

	/**
	 * Just a subclass for DatabaseException (which is abstract).
	 */
	class DummyDatabaseException extends DatabaseException
	{
		public DummyDatabaseException(final String message)
		{
			super(message);
		}
	}
}
