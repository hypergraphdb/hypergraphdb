package hgtest.storage.bdb.TransactionBDBImpl;

import com.sleepycat.db.Cursor;
import com.sleepycat.db.Environment;
import com.sleepycat.db.Transaction;
import org.hypergraphdb.storage.bdb.BDBTxCursor;
import org.hypergraphdb.storage.bdb.TransactionBDBImpl;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;

/**
 * @author Yuriy Sechko
 */
public class TransactionBDBImpl_attachCursorTest
{
	protected final Transaction fakeTransaction = PowerMock
			.createStrictMock(Transaction.class);
	protected final Environment fakeEnvironment = PowerMock
			.createStrictMock(Environment.class);

	@Test
	public void cursorIsNull() throws Exception
	{
		PowerMock.replayAll();

		final TransactionBDBImpl bdbTransaction = new TransactionBDBImpl(
				fakeTransaction, fakeEnvironment);

		final BDBTxCursor bdbCursor = bdbTransaction.attachCursor(null);

		assertNotNull(bdbCursor);
		PowerMock.verifyAll();
	}

	@Test
	public void cursorIsNullAndTransactionIsNull() throws Exception
	{
		PowerMock.replayAll();

		final TransactionBDBImpl bdbTransaction = TransactionBDBImpl
				.nullTransaction();

		final BDBTxCursor bdbCursor = bdbTransaction.attachCursor(null);

		assertNotNull(bdbCursor);
		PowerMock.verifyAll();
	}

	@Test
	public void cursorIsNotNull() throws Exception
	{
		final Cursor fakeCursor = PowerMock.createStrictMock(Cursor.class);
		PowerMock.replayAll();

		final TransactionBDBImpl bdbTransaction = new TransactionBDBImpl(
				fakeTransaction, fakeEnvironment);

		final BDBTxCursor bdbCursor = bdbTransaction.attachCursor(fakeCursor);

		assertNotNull(bdbCursor);
		PowerMock.verifyAll();
	}

	@Test
	public void cursorIsNotNullAndTransactionIsNull() throws Exception
	{
		final Cursor fakeCursor = PowerMock.createStrictMock(Cursor.class);
		PowerMock.replayAll();

		final TransactionBDBImpl bdbTransaction = TransactionBDBImpl
				.nullTransaction();

		final BDBTxCursor bdbCursor = bdbTransaction.attachCursor(fakeCursor);

		assertNotNull(bdbCursor);
		PowerMock.verifyAll();
	}
}
