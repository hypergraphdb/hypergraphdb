package hgtest.storage.bje.TransactionBJEImpl;

import com.sleepycat.je.*;
import org.easymock.EasyMock;
import org.hypergraphdb.storage.bje.TransactionBJEImpl;
import org.hypergraphdb.transaction.HGTransactionException;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.powermock.api.easymock.PowerMock;
import org.junit.Test;

import static hgtest.storage.bje.TestUtils.assertExceptions;
import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.rules.ExpectedException.none;

public class TransactionBJEImpl_commitTest
{

	@Rule
	public final ExpectedException below = none();

	@Test
	public void happyPath_whenTransactionIsNull_andThereAreNotCursorsAttached()
			throws Exception
	{
		final TransactionBJEImpl bjeTransaction = TransactionBJEImpl
				.nullTransaction();

		bjeTransaction.commit();
	}

	@Test
	public void happyPath_whenTransactionIsNull_andThereIsOneAttachedCursor()
			throws Exception
	{
		final Cursor cursor = createStrictMock(Cursor.class);
		replay(cursor);
		final TransactionBJEImpl bjeTransaction = TransactionBJEImpl
				.nullTransaction();
		bjeTransaction.attachCursor(cursor);

		bjeTransaction.commit();

		verify(cursor);
	}

	@Test
	public void happyPath_whenTransactionIsNull_andThereAreTwoAttachedCursors()
			throws Exception
	{
		final Cursor firstCursor = createStrictMock(Cursor.class);
		final Cursor secondCursor = createStrictMock(Cursor.class);

		replay(firstCursor, secondCursor);
		final TransactionBJEImpl bjeTransaction = TransactionBJEImpl
				.nullTransaction();
		bjeTransaction.attachCursor(firstCursor);
		bjeTransaction.attachCursor(secondCursor);

		bjeTransaction.commit();

		verify(firstCursor, secondCursor);
	}

	@Test
	public void happyPath_whenThereAreNotCursorsAttached() throws Exception
	{
		final Transaction fakeTransaction = createStrictMock(Transaction.class);
		fakeTransaction.commit();
		final Environment fakeEnvironment = createStrictMock(Environment.class);
		replay(fakeTransaction, fakeEnvironment);
		final TransactionBJEImpl bjeTransaction = new TransactionBJEImpl(
				fakeTransaction, fakeEnvironment);

		bjeTransaction.commit();

		verify(fakeTransaction, fakeEnvironment);
	}

	@Test
	public void happyPath_whenThereIsOneCursorAttached() throws Exception
	{
		final Cursor fakeCursor = createStrictMock(Cursor.class);
		fakeCursor.close();
		final Transaction fakeTransaction = createStrictMock(Transaction.class);
		fakeTransaction.commit();
		final Environment fakeEnvironment = createStrictMock(Environment.class);
		replay(fakeCursor, fakeTransaction, fakeEnvironment);
		final TransactionBJEImpl bjeTransaction = new TransactionBJEImpl(
				fakeTransaction, fakeEnvironment);
		bjeTransaction.attachCursor(fakeCursor);

		bjeTransaction.commit();

		verify(fakeCursor, fakeTransaction, fakeEnvironment);
	}

	@Test
	public void happyPath_whenThereAreTwoCursorsAttached() throws Exception
	{
		final Cursor firstCursor = createStrictMock(Cursor.class);
		firstCursor.close();
		final Cursor secondCursor = createStrictMock(Cursor.class);
		secondCursor.close();
		final Transaction fakeTransaction = createStrictMock(Transaction.class);
		fakeTransaction.commit();
		final Environment fakeEnvironment = createStrictMock(Environment.class);
		replay(firstCursor, secondCursor, fakeTransaction, fakeEnvironment);
		final TransactionBJEImpl bjeTransaction = new TransactionBJEImpl(
				fakeTransaction, fakeEnvironment);
		bjeTransaction.attachCursor(firstCursor);
		bjeTransaction.attachCursor(secondCursor);

		bjeTransaction.commit();

		verify(firstCursor, secondCursor, fakeTransaction, fakeEnvironment);
	}

	@Test
	public void wrapsDatabaseException_withTransactionException()
			throws Exception
	{
		final Transaction fakeTransaction = createStrictMock(Transaction.class);
		fakeTransaction.commit();
		expectLastCall().andThrow(
				new DatabaseNotFoundException(
						"This exception is thrown by fake transaction."));
		final Environment fakeEnvironment = createStrictMock(Environment.class);
		replay(fakeTransaction, fakeEnvironment);
		final TransactionBJEImpl bjeTransaction = new TransactionBJEImpl(
				fakeTransaction, fakeEnvironment);

		try
		{
			below.expect(HGTransactionException.class);
			below.expectMessage("Failed to commit transaction");
			bjeTransaction.commit();
		}
		finally
		{
			verify(fakeTransaction, fakeEnvironment);
		}
	}
}
