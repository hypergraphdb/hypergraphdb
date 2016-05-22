package hgtest.storage.bje.BJEStorageImplementation;

import static org.junit.Assert.*;

import org.hypergraphdb.HGException;
import org.junit.After;
import org.junit.Test;

public class BJEStorageImplementation_startupTest extends
		BJEStorageImplementationTestBasis
{

	@Test
	public void environmentIsTransactional() throws Exception
	{
		startup();

		final boolean isTransactional = storage.getConfiguration()
				.getDatabaseConfig().getTransactional();
		assertTrue(isTransactional);
	}

	@Test
	public void storageIsNotReadOnly() throws Exception
	{
		startup();

		final boolean isReadOnly = storage.getConfiguration()
				.getDatabaseConfig().getReadOnly();
		assertFalse(isReadOnly);
	}

	@Test
	public void checkDatabaseName() throws Exception
	{
		startup();

		final String databaseLocation = storage.getBerkleyEnvironment()
				.getHome().getPath();
		assertEquals(testDatabaseLocation, databaseLocation);
	}

	@Test
	public void exceptionWhileStartupOccurred() throws Exception
	{
		below.expect(HGException.class);
		below
				.expectMessage("Failed to initialize HyperGraph data store: java.lang.IllegalStateException: Throw exception in test case.");
		startup(1, new IllegalStateException("Throw exception in test case."));
	}

	@Test
	public void environmentIsNotTransactional() throws Exception
	{
		startupNonTransactional();
	}

	@After
	public void shutdown() throws Exception
	{
		super.shutdown();
	}
}
