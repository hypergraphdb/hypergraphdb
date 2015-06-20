package hgtest.storage.bdb.BDBTxCursor;

import com.sleepycat.db.Cursor;
import com.sleepycat.db.DatabaseException;
import org.easymock.EasyMock;
import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.bdb.BDBTxCursor;
import org.hypergraphdb.storage.bdb.TransactionBDBImpl;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

import static hgtest.storage.bdb.TestUtils.assertExceptions;


/**
 * @author Yuriy Sechko
 */
public class BDBTxCursor_closeTest
{
	@Test
	public void cursorIsNotOpened() throws Exception
	{
		final Cursor cursor = null;
		final TransactionBDBImpl tx = PowerMock
				.createStrictMock(TransactionBDBImpl.class);
		PowerMock.replayAll();

		final BDBTxCursor bdbCursor = new BDBTxCursor(cursor, tx);
		bdbCursor.close();

		PowerMock.verifyAll();
	}

	@Test
	public void cursorIsOpened() throws Exception
	{
		final Cursor cursor = PowerMock.createStrictMock(Cursor.class);
		cursor.close();
		final TransactionBDBImpl tx = PowerMock
				.createMock(TransactionBDBImpl.class);
		final BDBTxCursor bdbCursor = new BDBTxCursor(cursor, tx);
		recordRemoveCursor(tx, bdbCursor);
		PowerMock.replayAll();

		bdbCursor.close();

		PowerMock.verifyAll();
	}

	@Test
	public void txIsNull() throws Exception
	{
		final Cursor cursor = PowerMock.createStrictMock(Cursor.class);
		cursor.close();
		final TransactionBDBImpl tx = null;
		PowerMock.replayAll();

		final BDBTxCursor bdbCursor = new BDBTxCursor(cursor, tx);
		bdbCursor.close();

		PowerMock.verifyAll();
	}

	@Test
	public void innerCursorThrowsDatabaseException() throws Exception
	{
		final Cursor cursor = PowerMock.createStrictMock(Cursor.class);
		cursor.close();
		EasyMock.expectLastCall()
				.andThrow(
						new CustomDatabaseException(
								"This is custom DatabaseException"));
		final TransactionBDBImpl tx = PowerMock
				.createMock(TransactionBDBImpl.class);
		final BDBTxCursor bdbCursor = new BDBTxCursor(cursor, tx);
		recordRemoveCursor(tx, bdbCursor);
		PowerMock.replayAll();

		try
		{
			bdbCursor.close();
		}
		catch (Exception ex)
		{
			assertExceptions(
					ex,
					HGException.class,
					"hgtest.storage.bdb.BDBTxCursor.BDBTxCursor_closeTest$CustomDatabaseException",
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
		EasyMock.expectLastCall().andThrow(new IllegalStateException());
		final TransactionBDBImpl tx = PowerMock
				.createMock(TransactionBDBImpl.class);
		final BDBTxCursor bdbCursor = new BDBTxCursor(cursor, tx);
		recordRemoveCursor(tx, bdbCursor);
		PowerMock.replayAll();

		try
		{
			bdbCursor.close();
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

	private void recordRemoveCursor(final TransactionBDBImpl tx,
			final BDBTxCursor bdbCursor) throws Exception
	{
		final Method removeCursorMethod = tx.getClass().getDeclaredMethod(
				"removeCursor", BDBTxCursor.class);
		removeCursorMethod.setAccessible(true);
		removeCursorMethod.invoke(tx, bdbCursor);
	}

	class CustomDatabaseException extends DatabaseException
	{
		public CustomDatabaseException(final String message)
		{
			super(message);
		}
	}
}
