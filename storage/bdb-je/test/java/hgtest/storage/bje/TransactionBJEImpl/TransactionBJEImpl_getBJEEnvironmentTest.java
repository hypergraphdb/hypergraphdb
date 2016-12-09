package hgtest.storage.bje.TransactionBJEImpl;

import com.sleepycat.je.Environment;
import com.sleepycat.je.Transaction;
import org.hypergraphdb.storage.bje.TransactionBJEImpl;
import org.powermock.api.easymock.PowerMock;
import org.junit.Test;

import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TransactionBJEImpl_getBJEEnvironmentTest
{
	@Test
	public void returnsNull_whenEnvironmentIsNull() throws Exception
	{
		final Transaction fakeTransaction = createStrictMock(Transaction.class);
		replay(fakeTransaction);

		final TransactionBJEImpl bjeTransaction = new TransactionBJEImpl(
				fakeTransaction, null);

		final Environment actual = bjeTransaction.getBJEEnvironment();

		assertNull(actual);

		verify(fakeTransaction);
	}

	@Test
	public void returnsInstanceSpecifiedInConstructor_whenEnvironmentIsNotNull()
			throws Exception
	{
		final Environment expectedEnvironment = createStrictMock(Environment.class);
		final Transaction fakeTransaction = createStrictMock(Transaction.class);
		replay(expectedEnvironment, fakeTransaction);
		final TransactionBJEImpl bjeTransaction = new TransactionBJEImpl(
				fakeTransaction, expectedEnvironment);

		final Environment actualEnvironment = bjeTransaction.getBJEEnvironment();

		assertEquals( expectedEnvironment, actualEnvironment);

		verify(expectedEnvironment, fakeTransaction);
	}
}
