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

public class TransactionBJEImpl_getBJETransactionTest
{
	@Test
	public void returnsNull_whenTransactionIsNull() throws Exception
	{
		final Environment fakeEnvironment = createStrictMock(Environment.class);
		replay(fakeEnvironment);

		final TransactionBJEImpl bjeTransaction = new TransactionBJEImpl(null,
				fakeEnvironment);

		final Transaction actual = bjeTransaction.getBJETransaction();

		assertNull(actual);

        verify(fakeEnvironment);
	}

	@Test
	public void returnInstanceSpecifiedInConstructor_whenTransactionIsNotNull() throws Exception
	{
		final Transaction expectedTransaction = createStrictMock(Transaction.class);
		final Environment fakeEnvironment = createStrictMock(Environment.class);
		replay(expectedTransaction, fakeEnvironment);

		final TransactionBJEImpl bjeTransaction = new TransactionBJEImpl(
				expectedTransaction, fakeEnvironment);

		final Transaction actualTransaction = bjeTransaction.getBJETransaction();

		assertEquals(expectedTransaction, actualTransaction);

		verify(expectedTransaction, fakeEnvironment);
	}
}
