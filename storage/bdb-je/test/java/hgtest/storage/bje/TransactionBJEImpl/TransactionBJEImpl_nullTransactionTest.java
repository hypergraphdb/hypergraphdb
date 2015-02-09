package hgtest.storage.bje.TransactionBJEImpl;

import org.hypergraphdb.storage.bje.TransactionBJEImpl;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNull;

/**
 * @author Yuriy Sechko
 */
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
