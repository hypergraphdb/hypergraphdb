package hgtest.storage.bje.TransactionBJEImpl;

import com.sleepycat.je.*;
import org.hypergraphdb.storage.bje.TransactionBJEImpl;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

/**
 * @author Yuriy Sechko
 */
public class TransactionBJEImpl_constructorTest
{
	@Test
	public void transactionIsNull() throws Exception
	{
		final Environment fakeEnvironment = PowerMock
				.createStrictMock(Environment.class);
		PowerMock.replayAll();

		// no exception here
		new TransactionBJEImpl(null, fakeEnvironment);

		PowerMock.verifyAll();
	}

	@Test
	public void environmentIsNull() throws Exception
	{
		final Transaction fakeTransaction = PowerMock
				.createStrictMock(Transaction.class);
		PowerMock.replayAll();

		// no exception here
		new TransactionBJEImpl(fakeTransaction, null);

		PowerMock.verifyAll();
	}
}
