package hgtest.storage.bje.TransactionBJEImpl;

import com.sleepycat.je.*;
import org.hypergraphdb.storage.bje.TransactionBJEImpl;
import org.powermock.api.easymock.PowerMock;
import org.junit.Test;

import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

public class TransactionBJEImpl_constructorTest
{
	@Test
	public void doesNotFail_whenTransactionIsNull() throws Exception
	{
		final Environment fakeEnvironment = createStrictMock(Environment.class);
		replay(fakeEnvironment);

		// no exception here
		new TransactionBJEImpl(null, fakeEnvironment);

		verify(fakeEnvironment);
	}

	@Test
	public void doesNotFail_whenEnvironmentIsNull() throws Exception
	{
		final Transaction fakeTransaction = createStrictMock(Transaction.class);
		replay(fakeTransaction);

		// no exception here
		new TransactionBJEImpl(fakeTransaction, null);

		verify(fakeTransaction);
	}
}
