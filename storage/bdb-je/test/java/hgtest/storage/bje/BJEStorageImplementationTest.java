package hgtest.storage.bje;

import com.sleepycat.je.DatabaseConfig;



//import org.easymock.EasyMock;
//import org.hypergraphdb.HGConfiguration;
//import org.hypergraphdb.HGHandleFactory;
//import org.hypergraphdb.HGPersistentHandle;
//import org.hypergraphdb.HGStore;
//import org.hypergraphdb.handle.IntPersistentHandle;
//import org.hypergraphdb.storage.bje.BJEStorageImplementation;
//import com.sleepycat.je.Environment;
//import org.hypergraphdb.transaction.HGTransactionManager;
//import org.powermock.api.easymock.PowerMock;
//import org.powermock.core.classloader.annotations.PrepareForTest;
//import org.powermock.modules.testng.PowerMockTestCase;
//import org.testng.annotations.AfterMethod;
//import org.testng.annotations.BeforeClass;
//import org.testng.annotations.Test;
//
//import java.io.File;
//
//import static org.testng.Assert.assertEquals;
//import static org.testng.Assert.assertFalse;
//import static org.testng.Assert.assertTrue;
//import static org.testng.Assert.assertNull;

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

//@PrepareForTest(HGConfiguration.class)
public class BJEStorageImplementationTest //extends PowerMockTestCase
{
//	private static final String TEMP_DATABASE_DIRECTORY_SUFFIX = "hgtest.tmp";
//	private static final String HGHANDLEFACTORY_IMPLEMENTATION_CLASS_NAME = "org.hypergraphdb.handle.UUIDHandleFactory";
//
//	// Location for temporary directory for tests
//	private String testDatabaseLocation = System.getProperty("user.home")
//			+ File.separator + "hgtest.tmp";
//
//	// Mock objects used during tests.
//	// BJEStorageImplementation doesn't require any other dependencies
//	// to be injected before "startup" method call.
//	HGStore mockStore;
//	HGConfiguration mockConfiguration;
//
//	// Storage instance used during tests
//	BJEStorageImplementation storage = new BJEStorageImplementation();
//
//	@BeforeClass
//	// Creates mock objects used during tests.
//	// We use strict mocks so the order of calls will be verified.
//	private void createMocks() throws Exception
//	{
//		mockStore = PowerMock.createStrictMock(HGStore.class);
//		mockConfiguration = PowerMock.createStrictMock(HGConfiguration.class);
//	}
//
//	/**
//	 * Deletes temporary directory used during tests
//	 */
//	@AfterMethod
//	private void deleteTestDatabaseDirectory()
//	{
//		// Reset mocks to be sure that them don't affect future calls.
//		PowerMock.reset(mockStore, mockConfiguration);
//		// Delete test directory
//		final File testDatabaseDir = new File(testDatabaseLocation);
//		testDatabaseDir.delete();
//	}
//
//	// Records expectations for mocked objects behaviour.
//	// Here are only expectations needed for "startup" call.
//	// If other calls or the different count of calls are expected
//	// then override such expectations in you code after this method.
//	private void prepareMocksForStartup() throws Exception
//	{
//		// Here we use usual EasyMock API, PowerMock is required only for mock
//		// creation.
//		EasyMock.expect(mockConfiguration.getHandleFactory()).andReturn(
//				(HGHandleFactory) Class.forName(
//						HGHANDLEFACTORY_IMPLEMENTATION_CLASS_NAME)
//						.newInstance());
//		EasyMock.expect(mockConfiguration.isTransactional()).andReturn(true)
//				.times(2);
//		EasyMock.expect(mockStore.getDatabaseLocation()).andReturn(
//				TEMP_DATABASE_DIRECTORY_SUFFIX);
//	}
//
//	// Switches mocks into "replay" state.
//	// After "replay" mocks are ready for calls.
//	public void replayMocks() throws Exception
//	{
//		EasyMock.replay(mockStore, mockConfiguration);
//	}
//
//	// Verifies mocked objects behaviour.
//	private void verifyMocks() throws Exception
//	{
//		PowerMock.verifyAll();
//	}
//
//	@Test
//	public void testStartup() throws Exception
//	{
//		System.out.println("testStartup :: begin");
//		prepareMocksForStartup();
//		replayMocks();
//		storage.startup(mockStore, mockConfiguration);
//		// Check whether the tested method performed correctly
//		final DatabaseConfig actualDatabaseConfig = storage.getConfiguration()
//				.getDatabaseConfig();
//		final Environment actualEnvironment = storage.getBerkleyEnvironment();
//		assertTrue(actualDatabaseConfig.getTransactional(),
//				"Storage should be transactional");
//		assertFalse(actualDatabaseConfig.getReadOnly(),
//				"Storage should be not readonly");
//		assertTrue(actualEnvironment.isValid(), "Environment is not valid");
//		final String actualDatabaseLocation = actualEnvironment.getHome().getPath();
//		assertEquals(actualDatabaseLocation, TEMP_DATABASE_DIRECTORY_SUFFIX,
//				String.format("Database location should be %s but %s is",
//						TEMP_DATABASE_DIRECTORY_SUFFIX, actualDatabaseLocation));
//		assertTrue(new File(TEMP_DATABASE_DIRECTORY_SUFFIX).exists());
//		storage.shutdown();
//		verifyMocks();
//		System.out.println("testStartup :: OK");
//	}
//
//	@Test
//	public void testShutdown() throws Exception
//	{
//		System.out.println("testShutdown :: begin");
//		try
//		{
//			prepareMocksForStartup();
//			replayMocks();
//			storage.startup(mockStore, mockConfiguration);
//			storage.shutdown();
//			final Environment environment = storage.getBerkleyEnvironment();
//			// Check that environment is not open.
//			// If environment is not open then IllegalStateException occurs.
//			environment.getHome().getPath();
//		}
//		catch (Exception ex)
//		{
//			assertEquals("java.lang.IllegalStateException", ex.getClass()
//					.getName());
//		}
//		finally
//		{
//			verifyMocks();
//		}
//		System.out.println("testShutdown :: OK");
//	}
//
//	@Test
//	/**
//	 * In this test case can not test "store" method alone, 
//	 * "getData" method is required for verifying that data 
//	 * stored correctly.
//	 */
//	public void testStore() throws Exception
//	{
//		System.out.println("testStore :: begin");
//		prepareMocksForStartup();
//		// Mock the reference to Transaction manager.
//		final HGTransactionManager transactionManager = new HGTransactionManager(
//				storage.getTransactionFactory());
//		EasyMock.expect(mockStore.getTransactionManager())
//				.andReturn(transactionManager).times(2);
//		replayMocks();
//		storage.startup(mockStore, mockConfiguration);
//		// Store the simple array
//		final HGPersistentHandle handle = new IntPersistentHandle(1);
//		final byte[] data = new byte[] { 1, 2, 3 };
//		storage.store(handle, data);
//		// Get stored data and verify it
//		final byte[] actualData = storage.getData(handle);
//		assertEquals(actualData, data);
//		storage.shutdown();
//		verifyMocks();
//		System.out.println("testStore :: OK");
//	}
//
//	// @Test
//	// public void testStore2() throws Exception
//	// {
//	// throw new IllegalStateException("Not yet implemented");
//	// }
//
//	// @Test
//	// public void testRemoveLink() throws Exception
//	// {
//	// throw new IllegalStateException("Not yet implemented");
//	// }
//
//	//
//	// @Test
//	// public void testAddIncidenceLink() throws Exception
//	// {
//	// throw new IllegalStateException("Not yet implemented");
//	// }
//	//
//	// @Test
//	// public void testContainsLink() throws Exception
//	// {
//	// throw new IllegalStateException("Not yet implemented");
//	// }
//	//
//	@Test
//	public void testGetData() throws Exception
//	{
//		System.out.println("testGetData :: begin");
//		prepareMocksForStartup();
//		// Mock the reference to Transaction manager.
//		final HGTransactionManager transactionManager = new HGTransactionManager(
//				storage.getTransactionFactory());
//		EasyMock.expect(mockStore.getTransactionManager())
//				.andReturn(transactionManager).times(3);
//		replayMocks();
//		storage.startup(mockStore, mockConfiguration);
//		// Case 1: store data and then retrieve it by handle
//		final HGPersistentHandle handle = new IntPersistentHandle(1);
//		final byte[] data = new byte[] { 1, 2, 3 };
//		storage.store(handle, data);
//		final byte[] actualData = storage.getData(handle);
//		assertEquals(actualData, data);
//		// Case 2: try to get data which is not stored
//		final HGPersistentHandle nonStoredHandle = new IntPersistentHandle(2);
//		final byte[] nonStoredData = storage.getData(nonStoredHandle);
//		assertNull(nonStoredData);
//		storage.shutdown();
//		verifyMocks();
//		System.out.println("testGetData :: OK");
//	}
//
//	//
//	// @Test
//	// public void testGetIncidenceResultSet() throws Exception
//	// {
//	// throw new IllegalStateException("Not yet implemented");
//	// }
//	//
//	// @Test
//	// public void testGetIncidenceSetCardinality() throws Exception
//	// {
//	// throw new IllegalStateException("Not yet implemented");
//	// }
//	//
//	// @Test
//	// public void testGetLink() throws Exception
//	// {
//	// throw new IllegalStateException("Not yet implemented");
//	// }
//	//
//	// @Test
//	// public void testGetTransactionFactory() throws Exception
//	// {
//	// throw new IllegalStateException("Not yet implemented");
//	// }
//	//
//	@Test
//	public void testRemoveData() throws Exception
//	{
//		System.out.println("testRemoveData :: begin");
//		prepareMocksForStartup();
//		// Mock the reference to Transaction manager.
//		final HGTransactionManager transactionManager = new HGTransactionManager(
//				storage.getTransactionFactory());
//		EasyMock.expect(mockStore.getTransactionManager())
//				.andReturn(transactionManager).times(3);
//		replayMocks();
//		storage.startup(mockStore, mockConfiguration);
//		final HGPersistentHandle handle = new IntPersistentHandle(1);
//		final byte[] data = new byte[] { 1, 2, 3 };
//		storage.store(handle, data);
//		storage.removeData(handle);
//		final byte[] retrievedData = storage.getData(handle);
//		assertNull(retrievedData);
//		storage.shutdown();
//		verifyMocks();
//		System.out.println("testRemoveData :: OK");
//	}
//	//
//	// @Test
//	// public void testRemoveIncidenceLink() throws Exception
//	// {
//	// throw new IllegalStateException("Not yet implemented");
//	// }
//	//
//	// @Test
//	// public void testRemoveIncidenceSet() throws Exception
//	// {
//	// throw new IllegalStateException("Not yet implemented");
//	// }
//	//
//	// @Test
//	// public void testGetIndex() throws Exception
//	// {
//	// throw new IllegalStateException("Not yet implemented");
//	// }
//	//
//	// @Test
//	// public void testRemoveIndex() throws Exception
//	// {
//	// throw new IllegalStateException("Not yet implemented");
//	// }
//
//	// public static void main(String args[])
//	// {
//	// String databaseLocation = "/home/yura/hgdb/test";
//	// HyperGraph graph = null;
//	// try
//	// {
//	// graph = new HyperGraph(databaseLocation);
//	// String text = "This is a test";
//	// graph.add(text);
//	// HGPersistentHandle handle = new IntPersistentHandle(1);
//	// graph.add(handle);
//	// graph.remove(handle);
//	// HGPersistentHandle handle1 = graph.get(handle);
//	// System.out.println(handle1);
//	//
//	// }
//	// catch (Throwable t)
//	// {
//	// t.printStackTrace();
//	// }
//	// finally
//	// {
//	// graph.close();
//	// }
//	// }
}