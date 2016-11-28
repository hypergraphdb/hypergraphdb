package hgtest.storage.bje.TransactionBJEImpl;


import org.hypergraphdb.storage.bje.TransactionBJEImpl;
import org.junit.Test;

import static org.junit.Assert.assertNull;

public class TransactionBJEImpl_nullTransactionTest
{
	@Test
	public void test() throws Exception
	{
		final TransactionBJEImpl bjeTransaction = TransactionBJEImpl
				.nullTransaction();

		assertNull(bjeTransaction.getBJETransaction());
		assertNull(bjeTransaction.getBJEEnvironment());
	}
}
