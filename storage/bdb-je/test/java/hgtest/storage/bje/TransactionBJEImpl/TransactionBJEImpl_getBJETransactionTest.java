package hgtest.storage.bje.TransactionBJEImpl;

import com.sleepycat.je.Environment;
import com.sleepycat.je.Transaction;
import org.hypergraphdb.storage.bje.TransactionBJEImpl;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

/**
 * @author Yuriy Sechko
 */
public class TransactionBJEImpl_getBJETransactionTest
{
	@Test
	public void transactionIsNull() throws Exception
	{
		final Environment fakeEnvironment = PowerMock
				.createStrictMock(Environment.class);
		PowerMock.replayAll();

		final TransactionBJEImpl bjeTransaction = new TransactionBJEImpl(null,
				fakeEnvironment);

		final Transaction actual = bjeTransaction.getBJETransaction();

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

		final TransactionBJEImpl bjeTransaction = new TransactionBJEImpl(
				expected, fakeEnvironment);

		final Transaction actual = bjeTransaction.getBJETransaction();

		assertEquals(actual, expected);
		PowerMock.verifyAll();
	}
}
