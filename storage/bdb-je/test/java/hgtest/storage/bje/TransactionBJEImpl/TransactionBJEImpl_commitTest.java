package hgtest.storage.bje.TransactionBJEImpl;

import com.sleepycat.je.*;
import org.easymock.EasyMock;
import org.hypergraphdb.storage.bje.TransactionBJEImpl;
import org.hypergraphdb.transaction.HGTransactionException;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import static hgtest.storage.bje.TestUtils.assertExceptions;


/**
 * @author Yuriy Sechko
 */
public class TransactionBJEImpl_commitTest
{
	@Test
	public void transactionIsNullAndThereAreNotCursorsAttached()
			throws Exception
	{
		final TransactionBJEImpl bjeTransaction = TransactionBJEImpl
				.nullTransaction();

		bjeTransaction.commit();
	}

	@Test
	public void transactionIsNullAndThereIsOneAttachedCursor() throws Exception
	{
		final Cursor cursor = PowerMock.createStrictMock(Cursor.class);
		PowerMock.replayAll();
		final TransactionBJEImpl bjeTransaction = TransactionBJEImpl
				.nullTransaction();
		bjeTransaction.attachCursor(cursor);

		bjeTransaction.commit();

		PowerMock.verifyAll();
	}

	@Test
	public void transactionIsNullAndThereAreTwoAttachedCursors()
			throws Exception
	{
		final Cursor firstCursor = PowerMock.createStrictMock(Cursor.class);
		final Cursor secondCursor = PowerMock.createStrictMock(Cursor.class);

		PowerMock.replayAll();
		final TransactionBJEImpl bjeTransaction = TransactionBJEImpl
				.nullTransaction();
		bjeTransaction.attachCursor(firstCursor);
		bjeTransaction.attachCursor(secondCursor);

		bjeTransaction.commit();

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
		final TransactionBJEImpl bjeTransaction = new TransactionBJEImpl(
				fakeTransaction, fakeEnvironment);

		bjeTransaction.commit();

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
		final TransactionBJEImpl bjeTransaction = new TransactionBJEImpl(
				fakeTransaction, fakeEnvironment);
		bjeTransaction.attachCursor(cursor);

		bjeTransaction.commit();

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
		final TransactionBJEImpl bjeTransaction = new TransactionBJEImpl(
				fakeTransaction, fakeEnvironment);
		bjeTransaction.attachCursor(firstCursor);
		bjeTransaction.attachCursor(secondCursor);

		bjeTransaction.commit();

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
				new DatabaseNotFoundException(
						"This exception is thrown by fake transaction."));
		final Environment fakeEnvironment = PowerMock
				.createStrictMock(Environment.class);
		PowerMock.replayAll();
		final TransactionBJEImpl bjeTransaction = new TransactionBJEImpl(
				fakeTransaction, fakeEnvironment);

		try
		{
			bjeTransaction.commit();
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
