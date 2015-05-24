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
public class TransactionBDBImpl_getBDBEnvironmentTest
{
	@Test
	public void environmentIsNull() throws Exception
	{
		final Transaction fakeTransaction = PowerMock
				.createStrictMock(Transaction.class);
		PowerMock.replayAll();

		final TransactionBDBImpl bdbTransaction = new TransactionBDBImpl(
				fakeTransaction, null);

		final Environment actual = bdbTransaction.getBDBEnvironment();

		assertNull(actual);
		PowerMock.verifyAll();
	}

	@Test
	public void environmentIsNotNull() throws Exception
	{
		final Environment expected = PowerMock
				.createStrictMock(Environment.class);
		final Transaction fakeTransaction = PowerMock
				.createStrictMock(Transaction.class);
		PowerMock.replayAll();

		final TransactionBDBImpl bdbTransaction = new TransactionBDBImpl(
				fakeTransaction, expected);

		final Environment actual = bdbTransaction.getBDBEnvironment();

		assertEquals(actual, expected);
		PowerMock.verifyAll();
	}
}
