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
public class TransactionBJEImpl_getBJEEnvironmentTest
{
	@Test
	public void environmentIsNull() throws Exception
	{
		final Transaction fakeTransaction = PowerMock
				.createStrictMock(Transaction.class);
		PowerMock.replayAll();

		final TransactionBJEImpl bjeTransaction = new TransactionBJEImpl(
				fakeTransaction, null);

		final Environment actual = bjeTransaction.getBJEEnvironment();

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

		final TransactionBJEImpl bjeTransaction = new TransactionBJEImpl(
				fakeTransaction, expected);

		final Environment actual = bjeTransaction.getBJEEnvironment();

		assertEquals(actual, expected);
		PowerMock.verifyAll();
	}
}
