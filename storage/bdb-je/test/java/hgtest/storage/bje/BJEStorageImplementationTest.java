package hgtest.storage.bje;

import org.easymock.EasyMock;
import org.hypergraphdb.HGConfiguration;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGStore;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.handle.IntPersistentHandle;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.hypergraphdb.storage.bje.BJEStorageImplementation;
import com.sleepycat.je.Environment;
import org.hypergraphdb.transaction.HGTransactionManager;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertNull;

/**
 * This class contains unit tests for {@link BJEStorageImplementation}. For this
 * purpose we use PowerMock + EasyMock test framework (see <a
 * href="http://code.google.com/p/powermock/">PowerMock page on Google Code</a>
 * and <a href="http://easymock.org/">EasyMock home page</a> for details).
 * EasyMock's capabilities are sufficient for interfaces and plain classes. But
 * some classes in hypergraphdb modules are final so we cannot mock it in usual
 * way. PowerMock allows to create mocks even for final classes. So we can test
 * {@link BJEStorageImplementation} in isolation from other environment and in
 * most cases (if required) from other classes.
 * 
 * @author Yuriy Sechko
 */

@PrepareForTest(HGConfiguration.class)
public class BJEStorageImplementationTest extends PowerMockTestCase
{
	private static final String HGHANDLEFACTORY_IMPLEMENTATION_CLASS_NAME = "org.hypergraphdb.handle.UUIDHandleFactory";

	// location of temporary directory for tests
	private String testDatabaseLocation = System.getProperty("user.home")
			+ File.separator + "hgtest.tmp";

	// classes which are used by BJEStorageImplementation
	HGStore store;
	HGConfiguration configuration;

	final BJEStorageImplementation storage = new BJEStorageImplementation();

	@BeforeClass
	private void createMocks() throws Exception
	{
		store = PowerMock.createStrictMock(HGStore.class);
		configuration = PowerMock.createStrictMock(HGConfiguration.class);
	}

	@BeforeMethod
	@AfterMethod
	private void resetMocksAndDeleteTestDirectory()
	{
		PowerMock.reset(store, configuration);
		final File testDir = new File(testDatabaseLocation);
		final File[] filesInTestDir = testDir.listFiles();
		if (filesInTestDir != null)
		{
			for (final File eachFile : filesInTestDir)
			{
				eachFile.delete();
			}
		}
		testDir.delete();

	}

	private void mockConfiguration(final int calls) throws Exception
	{
		EasyMock.expect(configuration.getHandleFactory()).andReturn(
				(HGHandleFactory) Class.forName(
						HGHANDLEFACTORY_IMPLEMENTATION_CLASS_NAME)
						.newInstance());
		EasyMock.expect(configuration.isTransactional()).andReturn(true)
				.times(calls);
	}

	private void mockStore() throws Exception
	{
		EasyMock.expect(store.getDatabaseLocation()).andReturn(
				testDatabaseLocation);
	}

	private void mockTransactionManager(final int calls)
	{
		final HGTransactionManager transactionManager = new HGTransactionManager(
				storage.getTransactionFactory());
		EasyMock.expect(store.getTransactionManager())
				.andReturn(transactionManager).times(calls);
	}

	public void replay() throws Exception
	{
		EasyMock.replay(store, configuration);
	}

	private void verify() throws Exception
	{
		PowerMock.verifyAll();
	}

	@Test
	public void environmentIsTransactional() throws Exception
	{
		mockConfiguration(2);
		mockStore();
		replay();
		storage.startup(store, configuration);
		final boolean isTransactional = storage.getConfiguration()
				.getDatabaseConfig().getTransactional();
		assertTrue(isTransactional);
		storage.shutdown();
		verify();
	}

	@Test
	public void storageIsNotReadOnly() throws Exception
	{
		mockConfiguration(2);
		mockStore();
		replay();
		storage.startup(store, configuration);
		final boolean isReadOnly = storage.getConfiguration()
				.getDatabaseConfig().getReadOnly();
		assertFalse(isReadOnly);
		storage.shutdown();
		verify();
	}

	@Test
	public void databaseNameAsSpecifiedInStore() throws Exception
	{
		mockConfiguration(2);
		mockStore();
		replay();
		storage.startup(store, configuration);
		final String databaseLocation = storage.getBerkleyEnvironment()
				.getHome().getPath();
		assertEquals(databaseLocation, testDatabaseLocation);
		storage.shutdown();
		verify();
	}

	@Test
	public void getDatabasePathAfterShutdown() throws Exception
	{
		try
		{
			mockConfiguration(2);
			mockStore();
			replay();
			storage.startup(store, configuration);
			storage.shutdown();
			final Environment environment = storage.getBerkleyEnvironment();
			// environment is not open, expect exception
			environment.getHome().getPath();
		}
		catch (Exception ex)
		{
			assertEquals(ex.getClass(), IllegalStateException.class);
			assertEquals(ex.getMessage(),
					"Attempt to use non-open Environment object().");
		}
		finally
		{
			verify();
		}
	}

	@Test
	public void storeData() throws Exception
	{
		final byte[] expected = new byte[] { 1, 2, 3 };
		mockConfiguration(2);
		mockStore();
		mockTransactionManager(2);
		replay();
		storage.startup(store, configuration);
		final HGPersistentHandle handle = new IntPersistentHandle(1);
		storage.store(handle, new byte[] { 1, 2, 3 });
		final byte[] stored = storage.getData(handle);
		assertEquals(stored, expected);
		storage.shutdown();
		verify();
	}

	@Test
	public void readDataUsingHandle() throws Exception
	{
		final byte[] expected = new byte[] { 1, 2, 3 };
		mockConfiguration(2);
		mockStore();
		mockTransactionManager(2);
		replay();
		storage.startup(store, configuration);
		final HGPersistentHandle handle = new IntPersistentHandle(1);
		storage.store(handle, new byte[] { 1, 2, 3 });
		final byte[] stored = storage.getData(handle);
		assertEquals(stored, expected);
		storage.shutdown();
		verify();
	}

	@Test
	public void readDataWhichIsNotStoredUsingGivenHandle() throws Exception
	{
		mockConfiguration(2);
		mockStore();
		mockTransactionManager(1);
		replay();
		storage.startup(store, configuration);
		final HGPersistentHandle handle = new IntPersistentHandle(2);
		final byte[] retrievedData = storage.getData(handle);
		assertNull(retrievedData);
		storage.shutdown();
		verify();
	}

	@Test
	public void removeDataUsingHandle() throws Exception
	{
		mockConfiguration(2);
		mockStore();
		mockTransactionManager(3);
		replay();
		storage.startup(store, configuration);
		final HGPersistentHandle handle = new IntPersistentHandle(1);
		storage.store(handle, new byte[] { 1, 2, 3 });
		storage.removeData(handle);
		final byte[] retrievedData = storage.getData(handle);
		assertNull(retrievedData);
		storage.shutdown();
		verify();
	}

	@Test
	public void checkExistenceOfStoredData() throws Exception
	{
		mockConfiguration(2);
		mockStore();
		mockTransactionManager(2);
		replay();
		storage.startup(store, configuration);
		final HGPersistentHandle handle = new IntPersistentHandle(2);
		storage.store(handle, new byte[] { 4, 5, 6 });
		assertTrue(storage.containsData(handle));
		storage.shutdown();
		verify();
	}

	@Test
	public void checkExistenceOfNonStoredData() throws Exception
	{
		mockConfiguration(2);
		mockStore();
		mockTransactionManager(1);
		replay();
		storage.startup(store, configuration);
		final HGPersistentHandle handle = new IntPersistentHandle(2);
		assertFalse(storage.containsData(handle));
		storage.shutdown();
		verify();
	}

	@Test
	public void getIncidenceSetCardinalityUsingNullHandle() throws Exception
	{
		mockConfiguration(2);
		mockStore();
		replay();
		storage.startup(store, configuration);
		try
		{
			storage.getIncidenceSetCardinality(null);
		}
		catch (Exception ex)
		{
			assertEquals(ex.getClass(), NullPointerException.class);
			assertEquals(ex.getMessage(),
					"HGStore.getIncidenceSetCardinality called with a null handle.");
		}
		finally
		{
			storage.shutdown();
			verify();
		}
	}

	@Test
	public void noIncidenceLinksForNonStoredHandle() throws Exception
	{
		mockConfiguration(2);
		mockStore();
		mockTransactionManager(1);
		replay();
		storage.startup(store, configuration);
		final long cardinality = storage
				.getIncidenceSetCardinality(new IntPersistentHandle(2));
		assertEquals(cardinality, 0);
	}

	@Test
	public void createOneIncidenceLink() throws Exception
	{
		mockConfiguration(2);
		mockStore();
		mockTransactionManager(3);
		replay();
		storage.startup(store, configuration);
		final HGPersistentHandle handle = new IntPersistentHandle(1);
		storage.store(handle, new byte[] { 4, 5, 6 });
		storage.addIncidenceLink(handle, new IntPersistentHandle(2));
		final long cardinality = storage.getIncidenceSetCardinality(handle);
		assertEquals(cardinality, 1);
		storage.shutdown();
		verify();
	}

	@Test
	public void createTwoIncidenceLinks() throws Exception
	{
		mockConfiguration(2);
		mockStore();
		mockTransactionManager(4);
		replay();
		storage.startup(store, configuration);
		final HGPersistentHandle handle = new IntPersistentHandle(4);
		storage.store(handle, new byte[] {});
		storage.addIncidenceLink(handle, new IntPersistentHandle(25));
		storage.addIncidenceLink(handle, new IntPersistentHandle(26));
		final long cardinality = storage.getIncidenceSetCardinality(handle);
		assertEquals(cardinality, 2);
		storage.shutdown();
		verify();
	}

	@Test
	public void getLinksUsingNullHandle() throws Exception
	{
		mockConfiguration(2);
		mockStore();
		replay();
		storage.startup(store, configuration);
		try
		{
			storage.getLink(null);
		}
		catch (Exception ex)
		{
			assertEquals(ex.getClass(), org.hypergraphdb.HGException.class);
			assertEquals(ex.getMessage(),
					"Failed to retrieve link with handle null");
		}
		finally
		{
			storage.shutdown();
			verify();
		}
	}

	@Test
	public void getLinkWhichIsNotStored() throws Exception
	{
		mockConfiguration(2);
		mockStore();
		mockTransactionManager(1);
		replay();
		storage.startup(store, configuration);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		final HGPersistentHandle[] storedLinks = storage.getLink(handle);
		assertNull(storedLinks);
		storage.shutdown();
		verify();
	}

	@Test
	public void storeOneLink() throws Exception
	{
		mockConfiguration(2);
		mockStore();
		mockTransactionManager(2);
		replay();
		storage.startup(store, configuration);
		final HGPersistentHandle first = new UUIDPersistentHandle();
		final HGPersistentHandle second = new UUIDPersistentHandle();
		final HGPersistentHandle[] links = new HGPersistentHandle[] { second };
		storage.store(first, links);
		final HGPersistentHandle[] storedLinks = storage.getLink(first);
		assertEquals(storedLinks, links);
		storage.shutdown();
		verify();
	}

	@Test
	public void storeTwoLinks() throws Exception
	{
		mockConfiguration(2);
		mockStore();
		mockTransactionManager(2);
		replay();
		storage.startup(store, configuration);
		final HGPersistentHandle first = new UUIDPersistentHandle();
		final HGPersistentHandle[] links = new HGPersistentHandle[] {
				new UUIDPersistentHandle(), new UUIDPersistentHandle() };
		storage.store(first, links);
		final HGPersistentHandle[] storedLinks = storage.getLink(first);
		assertEquals(storedLinks, links);
		storage.shutdown();
		verify();
	}

	@Test
	public void checkExistenceOfStoredLinkFromFirstToSecond() throws Exception
	{
		mockConfiguration(2);
		mockStore();
		mockTransactionManager(2);
		replay();
		storage.startup(store, configuration);
		final HGPersistentHandle first = new UUIDPersistentHandle();
		final HGPersistentHandle second = new UUIDPersistentHandle();
		final HGPersistentHandle[] links = new HGPersistentHandle[] { second };
		storage.store(first, links);
		assertTrue(storage.containsLink(first));
		storage.shutdown();
		verify();
	}

	@Test
	public void checkExistenceOfStoredLinkFromSecondToFirst() throws Exception
	{
		mockConfiguration(2);
		mockStore();
		mockTransactionManager(2);
		replay();
		storage.startup(store, configuration);
		final HGPersistentHandle first = new UUIDPersistentHandle();
		final HGPersistentHandle second = new UUIDPersistentHandle();
		final HGPersistentHandle[] links = new HGPersistentHandle[] { second };
		storage.store(first, links);
		assertFalse(storage.containsLink(second));
		storage.shutdown();
		verify();
	}

	@Test
	public void checkExistenceOfHandleWhichIsLinkedToItself() throws Exception
	{
		mockConfiguration(2);
		mockStore();
		mockTransactionManager(2);
		replay();
		storage.startup(store, configuration);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.store(handle, new HGPersistentHandle[] { handle });
		assertTrue(storage.containsLink(handle));
		storage.shutdown();
		verify();
	}

	@Test
	public void checkExistenceOfNonStoredLink() throws Exception
	{
		mockConfiguration(2);
		mockStore();
		mockTransactionManager(1);
		replay();
		storage.startup(store, configuration);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		assertFalse(storage.containsLink(handle));
		storage.shutdown();
		verify();
	}

	@Test
	public void removeLinkUsingNullHandle() throws Exception
	{
		mockConfiguration(2);
		mockStore();
		replay();
		storage.startup(store, configuration);
		try
		{
			storage.removeLink(null);
		}
		catch (Exception ex)
		{
			assertEquals(ex.getClass(), NullPointerException.class);
			assertEquals(ex.getMessage(),
					"HGStore.remove called with a null handle.");
		}
		finally
		{
			storage.shutdown();
			verify();
		}
	}

	// public static void main(String args[])
	// {
	// String databaseLocation = "/home/yura/hgdb/test";
	// HyperGraph graph = null;
	// try
	// {
	// graph = new HyperGraph(databaseLocation);
	// String text = "This is a test";
	// final HGHandle textHandle = graph.add(text);
	//
	// HGPersistentHandle handle = new IntPersistentHandle(1);
	// graph.add(handle);
	// graph.remove(handle);
	// HGPersistentHandle handle1 = graph.get(handle);
	// System.out.println(handle1);
	//
	// }
	// catch (Throwable t)
	// {
	// t.printStackTrace();
	// }
	// finally
	// {
	// graph.close();
	// }
	// }
}