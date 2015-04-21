package hgtest.storage.bdb.BDBStorageImplementation;

import org.hypergraphdb.transaction.HGTransactionFactory;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;

/**
 * @author Yuriy Sechko
 */
public class BDBStorageImplementation_getTransactionFactoryTest extends
		BDBStorageImplementationTestBasis
{
	@Test
	public void getTransactionFactory() throws Exception
	{
		startup();
		final HGTransactionFactory transactionFactory = storage
				.getTransactionFactory();
		assertNotNull(transactionFactory);
		shutdown();
	}
}
