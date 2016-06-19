package hgtest.storage.bje.TransactionBJEImpl;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Environment;
import com.sleepycat.je.Transaction;
import org.hypergraphdb.storage.bje.BJETxCursor;
import org.hypergraphdb.storage.bje.TransactionBJEImpl;
import org.powermock.api.easymock.PowerMock;
import org.junit.Test;

import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertNotNull;

// TODO meaningful names here
public class TransactionBJEImpl_attachCursorTest
{
	protected final Transaction fakeTransaction = createStrictMock(Transaction.class);
	protected final Environment fakeEnvironment = createStrictMock(Environment.class);

	@Test
	public void cursorIsNull() throws Exception
	{
		replay(fakeTransaction, fakeEnvironment);

		final TransactionBJEImpl bjeTransaction = new TransactionBJEImpl(
				fakeTransaction, fakeEnvironment);

		// no exception here
		final BJETxCursor bjeCursor = bjeTransaction.attachCursor(null);

		assertNotNull(bjeCursor);

		verify(fakeTransaction, fakeEnvironment);
	}

	@Test
	public void cursorIsNullAndTransactionIsNull() throws Exception
	{
		replay(fakeTransaction, fakeEnvironment);

		final TransactionBJEImpl bjeTransaction = TransactionBJEImpl
				.nullTransaction();

		// no exception here
		final BJETxCursor bjeCursor = bjeTransaction.attachCursor(null);

		assertNotNull(bjeCursor);

		verify(fakeTransaction, fakeEnvironment);
	}

	@Test
	public void cursorIsNotNull() throws Exception
	{
		final Cursor fakeCursor = createStrictMock(Cursor.class);
		replay(fakeTransaction, fakeEnvironment, fakeCursor);

		final TransactionBJEImpl bjeTransaction = new TransactionBJEImpl(
				fakeTransaction, fakeEnvironment);

		final BJETxCursor bjeCursor = bjeTransaction.attachCursor(fakeCursor);

		assertNotNull(bjeCursor);

		verify(fakeTransaction, fakeEnvironment, fakeCursor);
	}

	@Test
	public void cursorIsNotNullAndTransactionIsNull() throws Exception
	{
		final Cursor fakeCursor = createStrictMock(Cursor.class);
		replay(fakeTransaction, fakeEnvironment, fakeCursor);

		final TransactionBJEImpl bjeTransaction = TransactionBJEImpl
				.nullTransaction();

		final BJETxCursor bjeCursor = bjeTransaction.attachCursor(fakeCursor);

		assertNotNull(bjeCursor);

		verify(fakeTransaction, fakeEnvironment, fakeCursor);
	}
}
