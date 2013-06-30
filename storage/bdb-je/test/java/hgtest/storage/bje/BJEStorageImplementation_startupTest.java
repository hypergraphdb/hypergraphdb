package hgtest.storage.bje;

import org.easymock.EasyMock;
import org.hypergraphdb.HGConfiguration;
import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.HGStore;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertNull;

/**
 */
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
		shutdown();
	}

	@Test
	public void storageIsNotReadOnly() throws Exception
	{
		startup();
		final boolean isReadOnly = storage.getConfiguration()
				.getDatabaseConfig().getReadOnly();
		assertFalse(isReadOnly);
		shutdown();
	}

	@Test
	public void checkDatabaseName() throws Exception
	{
		startup();
		final String databaseLocation = storage.getBerkleyEnvironment()
				.getHome().getPath();
		assertEquals(databaseLocation, testDatabaseLocation);
		shutdown();
	}

	@Test
	public void exceptionWhileStartupOccurred() throws Exception
	{
		mockStore();
		mockConfigurationToThrowException();
		replay();
		try
		{
			storage.startup(store, configuration);
		}
		catch (Exception ex)
		{
			assertEquals(ex.getClass(), org.hypergraphdb.HGException.class);
			assertEquals(
					ex.getMessage(),
					"Failed to initialize HyperGraph data store: java.lang.IllegalStateException: Throw exception in test case.");
		}
		finally
		{
			shutdown();
		}
	}

	private void mockConfigurationToThrowException() throws Exception
	{
		configuration = PowerMock.createStrictMock(HGConfiguration.class);
		EasyMock.expect(configuration.getHandleFactory()).andReturn(
				(HGHandleFactory) Class.forName(
						HGHANDLEFACTORY_IMPLEMENTATION_CLASS_NAME)
						.newInstance());
		EasyMock.expect(configuration.isTransactional()).andReturn(true);
		EasyMock.expect(configuration.isTransactional()).andThrow(
				new IllegalStateException("Throw exception in test case."));
	}

	@Test
	public void environmentIsNotTransactional() throws Exception
	{
		mockStore();
		mockNonTransactionalConfiguration();
		replay();
		storage.startup(store, configuration);
		shutdown();
	}

	private void mockNonTransactionalConfiguration() throws Exception
	{
		configuration = PowerMock.createStrictMock(HGConfiguration.class);
		EasyMock.expect(configuration.getHandleFactory()).andReturn(
				(HGHandleFactory) Class.forName(
						HGHANDLEFACTORY_IMPLEMENTATION_CLASS_NAME)
						.newInstance());
		EasyMock.expect(configuration.isTransactional()).andReturn(false)
				.times(2);
	}
}
