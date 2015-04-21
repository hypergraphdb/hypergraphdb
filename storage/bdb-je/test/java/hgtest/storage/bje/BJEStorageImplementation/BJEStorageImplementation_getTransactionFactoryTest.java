package hgtest.storage.bje.BJEStorageImplementation;

import org.hypergraphdb.transaction.HGTransactionFactory;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;

/**
 * In this test returned transaction factory is checked only for nullity.
 */
public class BJEStorageImplementation_getTransactionFactoryTest extends
		BJEStorageImplementationTestBasis
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
