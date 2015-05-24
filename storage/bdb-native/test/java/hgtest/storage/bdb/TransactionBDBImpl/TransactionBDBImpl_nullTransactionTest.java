package hgtest.storage.bdb.TransactionBDBImpl;

import org.hypergraphdb.storage.bdb.TransactionBDBImpl;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNull;

/**
 * @author Yuriy Sechko
 */
public class TransactionBDBImpl_nullTransactionTest
{
	@Test
	public void test() throws Exception
	{
		final TransactionBDBImpl bdbTransaction = TransactionBDBImpl
				.nullTransaction();

		assertNull(bdbTransaction.getBDBTransaction());
		assertNull(bdbTransaction.getBDBEnvironment());
	}
}
