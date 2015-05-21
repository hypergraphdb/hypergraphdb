package hgtest.storage.HGStorageImplementation;

import org.hypergraphdb.transaction.HGTransactionFactory;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;

/**
 * @author Yuriy Sechko
 */
public class HGStorageImplementation_getTransactionFactoryTest extends
		HGStorageImplementationTestBasis
{
	@Test(dataProvider = "configurations_0")
	public void getTransactionFactory(final Class configuration)
			throws Exception
	{
		initSpecificStorageImplementation(configuration);
		final HGTransactionFactory transactionFactory = storage
				.getTransactionFactory();
		assertNotNull(transactionFactory);
	}
}
