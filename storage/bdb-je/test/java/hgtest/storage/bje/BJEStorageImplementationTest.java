package hgtest.storage.bje;

import org.easymock.EasyMock;
import org.hypergraphdb.HGConfiguration;
import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.HGStore;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.hypergraphdb.storage.bje.BJEStorageImplementation;
import com.sleepycat.je.Environment;
import org.hypergraphdb.transaction.HGTransactionManager;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.annotations.AfterMethod;
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
 * some classes in hypergraphdb modules are final so we cannot startup it in
 * usual way. PowerMock allows to create mocks even for final classes. So we can
 * test {@link BJEStorageImplementation} in isolation from other environment and
 * in most cases (if required) from other classes.
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

	@BeforeMethod
	private void resetMocksAndDeleteTestDirectory()
	{
		PowerMock.resetAll();
		deleteTestDirectory();
	}

	@AfterMethod
	private void verifyMocksAndDeleteTestDirectory()
	{
		PowerMock.verifyAll();
		deleteTestDirectory();
	}

	private void deleteTestDirectory()
	{
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
		configuration = PowerMock.createStrictMock(HGConfiguration.class);
		EasyMock.expect(configuration.getHandleFactory()).andReturn(
				(HGHandleFactory) Class.forName(
						HGHANDLEFACTORY_IMPLEMENTATION_CLASS_NAME)
						.newInstance());
		EasyMock.expect(configuration.isTransactional()).andReturn(true)
				.times(calls);
	}

	private void mockStore() throws Exception
	{
		store = PowerMock.createStrictMock(HGStore.class);
		EasyMock.expect(store.getDatabaseLocation()).andReturn(
				testDatabaseLocation);
	}

	private void startup(final int configurationCalls) throws Exception
	{
		mockConfiguration(configurationCalls);
		mockStore();
		EasyMock.replay(store, configuration);
		storage.startup(store, configuration);
	}

	private void startup(final int configurationCalls,
			final int transactionManagerCalls) throws Exception
	{
		mockConfiguration(configurationCalls);
		mockStore();
		// mock transaction manager
		final HGTransactionManager transactionManager = new HGTransactionManager(
				storage.getTransactionFactory());
		EasyMock.expect(store.getTransactionManager())
				.andReturn(transactionManager).times(transactionManagerCalls);
		EasyMock.replay(store, configuration);
		storage.startup(store, configuration);
	}

	private void shutdown() throws Exception
	{
		storage.shutdown();
	}

	@Test
	public void environmentIsTransactional() throws Exception
	{
		startup(2);
		final boolean isTransactional = storage.getConfiguration()
				.getDatabaseConfig().getTransactional();
		assertTrue(isTransactional);
		shutdown();
	}

	@Test
	public void storageIsNotReadOnly() throws Exception
	{
		startup(2);
		final boolean isReadOnly = storage.getConfiguration()
				.getDatabaseConfig().getReadOnly();
		assertFalse(isReadOnly);
		shutdown();
	}

	@Test
	public void databaseNameAsSpecifiedInStore() throws Exception
	{
		startup(2);
		final String databaseLocation = storage.getBerkleyEnvironment()
				.getHome().getPath();
		assertEquals(databaseLocation, testDatabaseLocation);
		shutdown();
	}

	@Test
	public void getDatabasePathAfterShutdown() throws Exception
	{
		startup(2);
		storage.shutdown();
		final Environment environment = storage.getBerkleyEnvironment();
		try
		{
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
			shutdown();
		}
	}

	@Test
	public void storeAndReadDataUsingHandle() throws Exception
	{
		final byte[] expected = new byte[] { 4, 5, 6 };
		startup(2, 2);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.store(handle, new byte[] { 4, 5, 6 });
		final byte[] stored = storage.getData(handle);
		assertEquals(stored, expected);
		shutdown();
	}

	@Test
	public void readDataWhichIsNotStoredUsingGivenHandle() throws Exception
	{
		startup(2, 1);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		final byte[] retrievedData = storage.getData(handle);
		assertNull(retrievedData);
		shutdown();
	}

	@Test
	public void removeDataUsingHandle() throws Exception
	{
		startup(2, 3);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.store(handle, new byte[] { 1, 2, 3 });
		storage.removeData(handle);
		final byte[] retrievedData = storage.getData(handle);
		assertNull(retrievedData);
		shutdown();
	}

	@Test
	public void removeDataUsingNullHandle() throws Exception
	{
		startup(2);
		try
		{
			storage.removeData(null);
		}
		catch (Exception ex)
		{
			assertEquals(ex.getClass(), NullPointerException.class);
			assertEquals(ex.getMessage(),
					"HGStore.remove called with a null handle.");
		}
		finally
		{
			shutdown();
		}
	}

	@Test
	public void checkExistenceOfStoredDataUsingNullHandle() throws Exception
	{
		startup(2);
		try
		{
			storage.containsData(null);
		}
		catch (Exception ex)
		{
			assertEquals(ex.getClass(), NullPointerException.class);
			assertEquals(ex.getMessage(), null);
		}
		finally
		{
			shutdown();
		}
	}

	@Test
	public void checkExistenceOfStoredData() throws Exception
	{
		startup(2, 2);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.store(handle, new byte[] { 4, 5, 6 });
		assertTrue(storage.containsData(handle));
		shutdown();
	}

	@Test
	public void checkExistenceOfNonStoredData() throws Exception
	{
		startup(2, 1);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		assertFalse(storage.containsData(handle));
		shutdown();
	}

	@Test
	public void getIncidenceSetCardinalityUsingNullHandle() throws Exception
	{
		startup(2);
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
			shutdown();
		}
	}

	@Test
	public void noIncidenceLinksForNonStoredHandle() throws Exception
	{
		startup(2, 1);
		final long cardinality = storage
				.getIncidenceSetCardinality(new UUIDPersistentHandle());
		assertEquals(cardinality, 0);
		shutdown();
	}

	@Test
	public void createOneIncidenceLink() throws Exception
	{
		startup(2, 3);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.store(handle, new byte[] { 4, 5, 6 });
		storage.addIncidenceLink(handle, new UUIDPersistentHandle());
		final long cardinality = storage.getIncidenceSetCardinality(handle);
		assertEquals(cardinality, 1);
		shutdown();
	}

	@Test
	public void createTwoIncidenceLinks() throws Exception
	{
		startup(2, 4);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.store(handle, new byte[] {});
		storage.addIncidenceLink(handle, new UUIDPersistentHandle());
		storage.addIncidenceLink(handle, new UUIDPersistentHandle());
		final long cardinality = storage.getIncidenceSetCardinality(handle);
		assertEquals(cardinality, 2);
		shutdown();
	}

	@Test
	public void removeIncidenceSetUsingNullHandle() throws Exception
	{
		startup(2);
		try
		{
			storage.removeIncidenceSet(null);
		}
		catch (Exception ex)
		{
			assertEquals(ex.getClass(), org.hypergraphdb.HGException.class);
			assertEquals(ex.getMessage(),
					"Failed to remove incidence set of handle null: java.lang.NullPointerException");
		}
		finally
		{
			shutdown();
		}
	}

	@Test
	public void removeIncidenceSetWhichContainsOneLink() throws Exception
	{
		startup(2, 3);
		final HGPersistentHandle first = new UUIDPersistentHandle();
		final HGPersistentHandle second = new UUIDPersistentHandle();
		storage.addIncidenceLink(first, second);
		storage.removeIncidenceSet(first);
		final long afterRemoving = storage.getIncidenceSetCardinality(first);
		assertEquals(afterRemoving, 0);
		shutdown();
	}

	@Test
	public void removeIncidenceSetWhichContainsTwoLinks() throws Exception
	{
		startup(2, 4);
		final HGPersistentHandle first = new UUIDPersistentHandle();
		final HGPersistentHandle second = new UUIDPersistentHandle();
		final HGPersistentHandle third = new UUIDPersistentHandle();
		storage.addIncidenceLink(first, second);
		storage.addIncidenceLink(first, third);
		storage.removeIncidenceSet(first);
		final long afterRemoving = storage.getIncidenceSetCardinality(first);
		assertEquals(afterRemoving, 0);
		shutdown();
	}

	@Test
	public void removeIncidenceSetForLinkWhichIsNotStored() throws Exception
	{
		startup(2, 2);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.removeIncidenceSet(handle);
		final long afterRemoving = storage.getIncidenceSetCardinality(handle);
		assertEquals(afterRemoving, 0);
		shutdown();
	}

	@Test
	public void removeIncidenceSetForLinkWhichHasNotIncidenceLinks()
			throws Exception
	{
		startup(2, 3);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.store(handle, new byte[] {});
		storage.removeIncidenceSet(handle);
		final long afterRemoving = storage.getIncidenceSetCardinality(handle);
		assertEquals(afterRemoving, 0);
		shutdown();
	}

	// @Test
	// public void removeOneIncidenceLink() throws Exception
	// {
	// startup(2, 10);
	// final HGPersistentHandle first = new UUIDPersistentHandle();
	// final HGPersistentHandle second = new UUIDPersistentHandle();
	// storage.addIncidenceLink(first, second);
	// storage.removeIncidenceLink(first, second);
	// assertEquals(storage.getIncidenceSetCardinality(first), 0);
	// shutdown();
	// }

	@Test
	public void getLinksUsingNullHandle() throws Exception
	{
		startup(2);
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
			shutdown();
		}
	}

	@Test
	public void getLinkWhichIsNotStored() throws Exception
	{
		startup(2, 1);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		final HGPersistentHandle[] storedLinks = storage.getLink(handle);
		assertNull(storedLinks);
		shutdown();
	}

	@Test
	public void storeOneLink() throws Exception
	{
		startup(2, 2);
		final HGPersistentHandle first = new UUIDPersistentHandle();
		final HGPersistentHandle second = new UUIDPersistentHandle();
		final HGPersistentHandle[] links = new HGPersistentHandle[] { second };
		storage.store(first, links);
		final HGPersistentHandle[] storedLinks = storage.getLink(first);
		assertEquals(storedLinks, links);
		shutdown();
	}

	@Test
	public void storeTwoLinks() throws Exception
	{
		startup(2, 2);
		final HGPersistentHandle first = new UUIDPersistentHandle();
		final HGPersistentHandle[] links = new HGPersistentHandle[] {
				new UUIDPersistentHandle(), new UUIDPersistentHandle() };
		storage.store(first, links);
		final HGPersistentHandle[] storedLinks = storage.getLink(first);
		assertEquals(storedLinks, links);
		shutdown();
	}

	@Test
	public void checkExistenceOfStoredLinkFromFirstToSecond() throws Exception
	{
		startup(2, 2);
		final HGPersistentHandle first = new UUIDPersistentHandle();
		final HGPersistentHandle second = new UUIDPersistentHandle();
		final HGPersistentHandle[] links = new HGPersistentHandle[] { second };
		storage.store(first, links);
		assertTrue(storage.containsLink(first));
		shutdown();
	}

	@Test
	public void checkExistenceOfStoredLinkFromSecondToFirst() throws Exception
	{
		startup(2, 2);
		final HGPersistentHandle first = new UUIDPersistentHandle();
		final HGPersistentHandle second = new UUIDPersistentHandle();
		final HGPersistentHandle[] links = new HGPersistentHandle[] { second };
		storage.store(first, links);
		assertFalse(storage.containsLink(second));
		shutdown();
	}

	@Test
	public void checkExistenceOfHandleWhichIsLinkedToItself() throws Exception
	{
		startup(2, 2);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.store(handle, new HGPersistentHandle[] { handle });
		assertTrue(storage.containsLink(handle));
		shutdown();
	}

	@Test
	public void checkExistenceOfNonStoredLink() throws Exception
	{
		startup(2, 1);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		assertFalse(storage.containsLink(handle));
		shutdown();
	}

	@Test
	public void removeLinkUsingNullHandle() throws Exception
	{
		startup(2);
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
			shutdown();
		}
	}

	@Test
	public void removeLinkWhichIsStored() throws Exception
	{
		startup(2, 3);
		final HGPersistentHandle first = new UUIDPersistentHandle();
		final HGPersistentHandle second = new UUIDPersistentHandle();
		storage.store(first, new HGPersistentHandle[] { second });
		storage.removeLink(first);
		assertFalse(storage.containsLink(first));
		shutdown();

	}

	@Test
	public void removeLinkWhichIsNotStored() throws Exception
	{
		startup(2, 2);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.removeLink(handle);
		assertFalse(storage.containsLink(handle));
		shutdown();
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