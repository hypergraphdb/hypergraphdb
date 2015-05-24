package hgtest.storage.bdb.TransactionBDBImpl;

import com.sleepycat.db.Environment;
import com.sleepycat.db.Transaction;
import org.hypergraphdb.storage.bdb.TransactionBDBImpl;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

/**
 * @author Yuriy Sechko
 */
public class TransactionBDBImpl_constructorTest
{
	@Test
	public void transactionIsNull() throws Exception
	{
		final Environment fakeEnvironment = PowerMock
				.createStrictMock(Environment.class);
		PowerMock.replayAll();

		new TransactionBDBImpl(null, fakeEnvironment);

		PowerMock.verifyAll();
	}

	@Test
	public void environmentIsNull() throws Exception
	{
		final Transaction fakeTransaction = PowerMock
				.createStrictMock(Transaction.class);
		PowerMock.replayAll();

		new TransactionBDBImpl(fakeTransaction, null);

		PowerMock.verifyAll();
	}
}
