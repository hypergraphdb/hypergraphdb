package hgtest.storage.bje.TransactionBJEImpl;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.Transaction;
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

// TODO meaningful names for test cases
public class TransactionBJEImpl_abortTest
{
	@Rule
	public final ExpectedException below = ExpectedException.none();

	@Test
	public void happyPath_whenTransactionIsNull_andThereAreNotCursorsAttached()
			throws Exception
	{
		final TransactionBJEImpl bjeTransaction = TransactionBJEImpl
				.nullTransaction();

		bjeTransaction.abort();
	}

	@Test
	public void happyPath_whenTransactionIsNull_andThereIsOneCursorAttached()
			throws Exception
	{
		final Cursor cursor = createStrictMock(Cursor.class);
		replay(cursor);

		final TransactionBJEImpl bjeTransaction = TransactionBJEImpl
				.nullTransaction();
		bjeTransaction.attachCursor(cursor);

		bjeTransaction.abort();

		verify(cursor);
	}

	@Test
	public void happyPath_whenTransactionIsNull_andThereAreTwoCursorsAttached()
			throws Exception
	{
		final Cursor firstCursor = createStrictMock(Cursor.class);
		final Cursor secondCursor = createStrictMock(Cursor.class);
		replay(firstCursor, secondCursor);
		final TransactionBJEImpl bjeTransaction = TransactionBJEImpl
				.nullTransaction();
		bjeTransaction.attachCursor(firstCursor);
		bjeTransaction.attachCursor(secondCursor);

		bjeTransaction.abort();

		verify(firstCursor, secondCursor);
	}

	@Test
	public void happyPath_whenThereAreNotCursorsAttached() throws Exception
	{
		final Transaction fakeTransaction = createStrictMock(Transaction.class);
		fakeTransaction.abort();
		final Environment fakeEnvironment = createStrictMock(Environment.class);
		replay(fakeTransaction, fakeEnvironment);
		final TransactionBJEImpl bjeTransaction = new TransactionBJEImpl(
				fakeTransaction, fakeEnvironment);

		bjeTransaction.abort();

		verify(fakeTransaction, fakeEnvironment);
	}

	@Test
	public void happyPath_whenThereIsOneCursorAttached() throws Exception
	{
		final Cursor cursor = createStrictMock(Cursor.class);
		cursor.close();
		final Transaction fakeTransaction = createStrictMock(Transaction.class);
		fakeTransaction.abort();
		final Environment fakeEnvironment = createStrictMock(Environment.class);
		replay(cursor, fakeTransaction, fakeEnvironment);
		final TransactionBJEImpl bjeTransaction = new TransactionBJEImpl(
				fakeTransaction, fakeEnvironment);
		bjeTransaction.attachCursor(cursor);

		bjeTransaction.abort();

		verify(cursor, fakeTransaction, fakeEnvironment);
	}

	@Test
	public void happyPath_whenThereAreTwoCursorsAttached() throws Exception
	{
		final Cursor firstCursor = createStrictMock(Cursor.class);
		firstCursor.close();
		final Cursor secondCursor = createStrictMock(Cursor.class);
		secondCursor.close();

		final Transaction fakeTransaction = createStrictMock(Transaction.class);
		fakeTransaction.abort();
		final Environment fakeEnvironment = createStrictMock(Environment.class);
		replay(firstCursor, secondCursor, fakeTransaction, fakeEnvironment);
		final TransactionBJEImpl bjeTransaction = new TransactionBJEImpl(
				fakeTransaction, fakeEnvironment);
		bjeTransaction.attachCursor(firstCursor);
		bjeTransaction.attachCursor(secondCursor);

		bjeTransaction.abort();

		verify(firstCursor, secondCursor, fakeTransaction, fakeEnvironment);
	}

	@Test
	public void wrapsUnderlyingExceptionWithHypergraphException()
			throws Exception
	{
		final Transaction fakeTransaction = createStrictMock(Transaction.class);
		fakeTransaction.abort();
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
			below.expectMessage("Failed to abort transaction");
			bjeTransaction.abort();
		}
		finally
		{
			verify(fakeTransaction, fakeEnvironment);
		}
	}
}
