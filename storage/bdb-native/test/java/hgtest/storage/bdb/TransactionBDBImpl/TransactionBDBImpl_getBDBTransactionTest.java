package hgtest.storage.bdb.TransactionBDBImpl;

import com.sleepycat.db.Environment;
import com.sleepycat.db.Transaction;
import org.hypergraphdb.storage.bdb.TransactionBDBImpl;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

/**
 * @author Yuriy Sechko
 */
public class TransactionBDBImpl_getBDBTransactionTest
{
	@Test
	public void transactionIsNull() throws Exception
	{
		final Environment fakeEnvironment = PowerMock
				.createStrictMock(Environment.class);
		PowerMock.replayAll();

		final TransactionBDBImpl bdbTransaction = new TransactionBDBImpl(null,
				fakeEnvironment);

		final Transaction actual = bdbTransaction.getBDBTransaction();

		assertNull(actual);
		PowerMock.verifyAll();
	}

	@Test
	public void transactionIsNotNull() throws Exception
	{
		final Transaction expected = PowerMock
				.createStrictMock(Transaction.class);
		final Environment fakeEnvironment = PowerMock
				.createStrictMock(Environment.class);
		PowerMock.replayAll();

		final TransactionBDBImpl bdbTransaction = new TransactionBDBImpl(
				expected, fakeEnvironment);

		final Transaction actual = bdbTransaction.getBDBTransaction();

		assertEquals(actual, expected);
		PowerMock.verifyAll();
	}
}
