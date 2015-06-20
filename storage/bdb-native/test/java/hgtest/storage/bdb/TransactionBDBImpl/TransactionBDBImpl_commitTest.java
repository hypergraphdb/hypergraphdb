package hgtest.storage.bdb.TransactionBDBImpl;

import com.sleepycat.db.Cursor;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.Environment;
import com.sleepycat.db.Transaction;
import org.easymock.EasyMock;
import org.hypergraphdb.storage.bdb.TransactionBDBImpl;
import org.hypergraphdb.transaction.HGTransactionException;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import static hgtest.storage.bdb.TestUtils.assertExceptions;


/**
 * @author Yuriy Sechko
 */
public class TransactionBDBImpl_commitTest
{
	@Test
	public void transactionIsNullAndThereAreNotCursorsAttached()
			throws Exception
	{
		final TransactionBDBImpl bdbTransaction = TransactionBDBImpl
				.nullTransaction();

		bdbTransaction.commit();
	}

	@Test
	public void transactionIsNullAndThereIsOneAttachedCursor() throws Exception
	{
		final Cursor cursor = PowerMock.createStrictMock(Cursor.class);
		PowerMock.replayAll();
		final TransactionBDBImpl bdbTransaction = TransactionBDBImpl
				.nullTransaction();
		bdbTransaction.attachCursor(cursor);

		bdbTransaction.commit();

		PowerMock.verifyAll();
	}

	@Test
	public void transactionIsNullAndThereAreTwoAttachedCursors()
			throws Exception
	{
		final Cursor firstCursor = PowerMock.createStrictMock(Cursor.class);
		final Cursor secondCursor = PowerMock.createStrictMock(Cursor.class);

		PowerMock.replayAll();
		final TransactionBDBImpl bdbTransaction = TransactionBDBImpl
				.nullTransaction();
		bdbTransaction.attachCursor(firstCursor);
		bdbTransaction.attachCursor(secondCursor);

		bdbTransaction.commit();

		PowerMock.verifyAll();
	}

	@Test
	public void thereAreNotCursorsAttached() throws Exception
	{
		final Transaction fakeTransaction = PowerMock
				.createStrictMock(Transaction.class);
		fakeTransaction.commit();
		final Environment fakeEnvironment = PowerMock
				.createStrictMock(Environment.class);
		PowerMock.replayAll();
		final TransactionBDBImpl bdbTransaction = new TransactionBDBImpl(
				fakeTransaction, fakeEnvironment);

		bdbTransaction.commit();

		PowerMock.verifyAll();
	}

	@Test
	public void thereIsOneCursorAttached() throws Exception
	{
		final Cursor cursor = PowerMock.createStrictMock(Cursor.class);
		cursor.close();
		final Transaction fakeTransaction = PowerMock
				.createStrictMock(Transaction.class);
		fakeTransaction.commit();
		final Environment fakeEnvironment = PowerMock
				.createStrictMock(Environment.class);
		PowerMock.replayAll();
		final TransactionBDBImpl bdbTransaction = new TransactionBDBImpl(
				fakeTransaction, fakeEnvironment);
		bdbTransaction.attachCursor(cursor);

		bdbTransaction.commit();

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
		fakeTransaction.commit();
		final Environment fakeEnvironment = PowerMock
				.createStrictMock(Environment.class);
		PowerMock.replayAll();
		final TransactionBDBImpl bdbTransaction = new TransactionBDBImpl(
				fakeTransaction, fakeEnvironment);
		bdbTransaction.attachCursor(firstCursor);
		bdbTransaction.attachCursor(secondCursor);

		bdbTransaction.commit();

		PowerMock.verifyAll();
	}

	@Test
	public void fakeTransactionThrowsExceptionOnCommit() throws Exception
	{
		final Exception expected = new HGTransactionException(
				"Failed to commit transaction");

		final Transaction fakeTransaction = PowerMock
				.createStrictMock(Transaction.class);
		fakeTransaction.commit();
		EasyMock.expectLastCall().andThrow(
				new DatabaseException(
						"This exception is thrown by fake transaction."));
		final Environment fakeEnvironment = PowerMock
				.createStrictMock(Environment.class);
		PowerMock.replayAll();
		final TransactionBDBImpl bdbTransaction = new TransactionBDBImpl(
				fakeTransaction, fakeEnvironment);

		try
		{
			bdbTransaction.commit();
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
