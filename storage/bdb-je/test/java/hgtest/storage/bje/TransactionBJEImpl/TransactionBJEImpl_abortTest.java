package hgtest.storage.bje.TransactionBJEImpl;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.Transaction;
import org.easymock.EasyMock;
import org.hypergraphdb.storage.bje.TransactionBJEImpl;
import org.hypergraphdb.transaction.HGTransactionException;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import static hgtest.storage.bje.TestUtils.assertExceptions;


/**
 * @author Yuriy Sechko
 */
public class TransactionBJEImpl_abortTest
{
	@Test
	public void transactionIsNullAndThereAreNotCursorsAttached()
			throws Exception
	{
		final TransactionBJEImpl bjeTransaction = TransactionBJEImpl
				.nullTransaction();

		bjeTransaction.abort();
	}

	@Test
	public void transactionIsNullAndThereIsOneCursorAttached() throws Exception
	{
		final Cursor cursor = PowerMock.createStrictMock(Cursor.class);
		PowerMock.replayAll();

		final TransactionBJEImpl bjeTransaction = TransactionBJEImpl
				.nullTransaction();
		bjeTransaction.attachCursor(cursor);

		bjeTransaction.abort();

		PowerMock.verifyAll();
	}

	@Test
	public void transactionIsNullAndThereAreTwoCursorsAttached()
			throws Exception
	{
		final Cursor firstCursor = PowerMock.createStrictMock(Cursor.class);
		final Cursor secondCursor = PowerMock.createStrictMock(Cursor.class);
		PowerMock.replayAll();
		final TransactionBJEImpl bjeTransaction = TransactionBJEImpl
				.nullTransaction();
		bjeTransaction.attachCursor(firstCursor);
		bjeTransaction.attachCursor(secondCursor);

		bjeTransaction.abort();

		PowerMock.verifyAll();
	}

	@Test
	public void thereAreNotCursorsAttached() throws Exception
	{
		final Transaction fakeTransaction = PowerMock
				.createStrictMock(Transaction.class);
		fakeTransaction.abort();
		final Environment fakeEnvironment = PowerMock
				.createStrictMock(Environment.class);
		PowerMock.replayAll();
		final TransactionBJEImpl bjeTransaction = new TransactionBJEImpl(
				fakeTransaction, fakeEnvironment);

		bjeTransaction.abort();

		PowerMock.verifyAll();
	}

	@Test
	public void thereIsOneCursorAttached() throws Exception
	{
		final Cursor cursor = PowerMock.createStrictMock(Cursor.class);
		cursor.close();
		final Transaction fakeTransaction = PowerMock
				.createStrictMock(Transaction.class);
		fakeTransaction.abort();
		final Environment fakeEnvironment = PowerMock
				.createStrictMock(Environment.class);
		PowerMock.replayAll();
		final TransactionBJEImpl bjeTransaction = new TransactionBJEImpl(
				fakeTransaction, fakeEnvironment);
		bjeTransaction.attachCursor(cursor);

		bjeTransaction.abort();

		PowerMock.verifyAll();
	}

	@Test
	public void thereAreTwoCursorsAttached() throws Exception
	{
		final Cursor firstCursor = PowerMock.createStrictMock(Cursor.class);
		firstCursor.close();
		final Cursor secondCursor = PowerMock.createStrictMock(Cursor.class);
		secondCursor.close();

		final Transaction fakeTransaction = PowerMock
				.createStrictMock(Transaction.class);
		fakeTransaction.abort();
		final Environment fakeEnvironment = PowerMock
				.createStrictMock(Environment.class);
		PowerMock.replayAll();
		final TransactionBJEImpl bjeTransaction = new TransactionBJEImpl(
				fakeTransaction, fakeEnvironment);
		bjeTransaction.attachCursor(firstCursor);
		bjeTransaction.attachCursor(secondCursor);

		bjeTransaction.abort();

		PowerMock.verifyAll();
	}

	@Test
	public void fakeTransactionThrowsExceptionOnAbort() throws Exception
	{
		final Exception expected = new HGTransactionException(
				"Failed to abort transaction");

		final Transaction fakeTransaction = PowerMock
				.createStrictMock(Transaction.class);
		fakeTransaction.abort();
		EasyMock.expectLastCall().andThrow(
				new DatabaseNotFoundException(
						"This exception is thrown by fake transaction."));
		final Environment fakeEnvironment = PowerMock
				.createStrictMock(Environment.class);
		PowerMock.replayAll();
		final TransactionBJEImpl bjeTransaction = new TransactionBJEImpl(
				fakeTransaction, fakeEnvironment);

		try
		{
			bjeTransaction.abort();
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
}
