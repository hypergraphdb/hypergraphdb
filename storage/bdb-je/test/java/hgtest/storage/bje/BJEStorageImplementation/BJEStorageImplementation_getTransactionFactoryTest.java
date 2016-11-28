package hgtest.storage.bje.BJEStorageImplementation;

import static org.junit.Assert.assertNotNull;

import org.hypergraphdb.transaction.HGTransactionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BJEStorageImplementation_getTransactionFactoryTest extends
		BJEStorageImplementationTestBasis
{
	@Test
	public void getTransactionFactory() throws Exception
	{
		final HGTransactionFactory transactionFactory = storage
				.getTransactionFactory();

		assertNotNull(transactionFactory);
	}

	@Before
	public void startup() throws Exception
	{
		super.startup();
	}

	@After
	public void shutdown() throws Exception
	{
		super.shutdown();
	}
}
