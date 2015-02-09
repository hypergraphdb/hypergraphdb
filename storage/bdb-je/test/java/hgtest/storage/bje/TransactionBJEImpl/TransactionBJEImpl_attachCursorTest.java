package hgtest.storage.bje.TransactionBJEImpl;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Environment;
import com.sleepycat.je.Transaction;
import org.hypergraphdb.storage.bje.BJETxCursor;
import org.hypergraphdb.storage.bje.TransactionBJEImpl;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;

/**
 * @author Yuriy Sechko
 */
public class TransactionBJEImpl_attachCursorTest
{
	protected final Transaction fakeTransaction = PowerMock
			.createStrictMock(Transaction.class);
	protected final Environment fakeEnvironment = PowerMock
			.createStrictMock(Environment.class);

	@Test
	public void cursorIsNull() throws Exception
	{
		PowerMock.replayAll();

		final TransactionBJEImpl bjeTransaction = new TransactionBJEImpl(
				fakeTransaction, fakeEnvironment);

		// no exception here
		final BJETxCursor bjeCursor = bjeTransaction.attachCursor(null);

		assertNotNull(bjeCursor);
		PowerMock.verifyAll();
	}

	@Test
	public void cursorIsNullAndTransactionIsNull() throws Exception
	{
		PowerMock.replayAll();

		final TransactionBJEImpl bjeTransaction = TransactionBJEImpl
				.nullTransaction();

		// no exception here
		final BJETxCursor bjeCursor = bjeTransaction.attachCursor(null);

		assertNotNull(bjeCursor);
		PowerMock.verifyAll();
	}

	@Test
	public void cursorIsNotNull() throws Exception
	{
		final Cursor fakeCursor = PowerMock.createStrictMock(Cursor.class);
		PowerMock.replayAll();

		final TransactionBJEImpl bjeTransaction = new TransactionBJEImpl(
				fakeTransaction, fakeEnvironment);

		final BJETxCursor bjeCursor = bjeTransaction.attachCursor(fakeCursor);

		assertNotNull(bjeCursor);
		PowerMock.verifyAll();
	}

	@Test
	public void cursorIsNotNullAndTransactionIsNull() throws Exception
	{
		final Cursor fakeCursor = PowerMock.createStrictMock(Cursor.class);
		PowerMock.replayAll();

		final TransactionBJEImpl bjeTransaction = TransactionBJEImpl
				.nullTransaction();

		final BJETxCursor bjeCursor = bjeTransaction.attachCursor(fakeCursor);

		assertNotNull(bjeCursor);
		PowerMock.verifyAll();
	}
}
