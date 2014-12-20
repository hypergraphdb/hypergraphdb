package hgtest.storage.bje.BJEStorageImplementation;

import org.easymock.EasyMock;
import org.hypergraphdb.HGConfiguration;
import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.HGStore;
import org.hypergraphdb.storage.bje.BJEStorageImplementation;
import org.hypergraphdb.transaction.HGTransactionManager;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertNull;

/**
 * <p>
 * This class used as basis for writing test cases for
 * {@link BJEStorageImplementation}. PowerMock + EasyMock frameworks are used.
 * (see <a href="http://code.google.com/p/powermock/">PowerMock page on Google
 * Code</a> and <a href="http://easymock.org/">EasyMock home page</a> for
 * details). EasyMock's capabilities are sufficient for interfaces and plain
 * classes. But some classes in hypergraphdb modules are final so we cannot mock
 * them in usual way. PowerMock allows to create mocks even for final classes.
 * So we can test {@link BJEStorageImplementation} in isolation from other
 * environment and in most cases from other classes.
 * </p>
 * <p>
 * Typical test case executes through several phases like initialization,
 * executing and verification.
 * </p>
 * <p>
 * {@link BJEStorageImplementation} requires two parameters before it can start
 * functioning: {@link org.hypergraphdb.HGStore} and
 * {@link org.hypergraphdb.HGConfiguration}. As mentioned above we test
 * {@link BJEStorageImplementation} in isolation. So the
 * {@link org.hypergraphdb.HGStore} and {@link org.hypergraphdb.HGConfiguration}
 * parameters should be mocked. It is done in
 * {@link BJEStorageImplementationTestBasis#mockStore()} and
 * {@link BJEStorageImplementationTestBasis#mockConfiguration(int)} methods.
 * </p>
 * <p>
 * The inner code in {@link BJEStorageImplementation} makes several calls to the
 * {@link org.hypergraphdb.HGStore} and {@link org.hypergraphdb.HGConfiguration}
 * objects. For {@link org.hypergraphdb.HGStore} only getDatabaseLocation() is
 * in use.
 * </p>
 * <p>
 * Mocking {@link org.hypergraphdb.HGConfiguration} object is more complicated.
 * Here we need to specify handle factory and {@code isTransactional} parameter.
 * In different test cases the number of calls may vary (For example if we test
 * reading the data we firstly store it and then read it. Each time we
 * store/read {@link org.hypergraphdb.HGConfiguration#isTransactional()} is
 * called). So number the of calls is passed using parameter {@code calls}.
 * </p>
 * <p>
 * In some test cases we need to test behavior of
 * {@link BJEStorageImplementation} when method is not executed properly and
 * exception somewhere inside is thrown or transaction was not opened etc. For
 * such test cases can be used specific initialization methods (like
 * {@link hgtest.storage.bje.BJEStorageImplementation.BJEStorageImplementationTestBasis#startup(int, java.lang.Exception)}
 * and
 * {@link hgtest.storage.bje.BJEStorageImplementation.BJEStorageImplementationTestBasis#startupNonTransactional()
 * )}.
 * </p>
 * 
 * Also this class contains some utility methods.
 * 
 * @author Yuriy Sechko
 */

@PrepareForTest(HGConfiguration.class)
public class BJEStorageImplementationTestBasis extends PowerMockTestCase
{
	protected static final String HGHANDLEFACTORY_IMPLEMENTATION_CLASS_NAME = "org.hypergraphdb.handle.UUIDHandleFactory";

	// location of temporary directory for tests
	final protected String testDatabaseLocation = System
			.getProperty("user.home") + File.separator + "hgtest.tmp";

	// classes which are used by BJEStorageImplementation
	HGStore store;
	HGConfiguration configuration;

	final BJEStorageImplementation storage = new BJEStorageImplementation();

	@BeforeMethod
	protected void resetMocksAndDeleteTestDirectory()
	{
		PowerMock.resetAll();
		deleteTestDirectory();
	}

	@AfterMethod
	protected void verifyMocksAndDeleteTestDirectory()
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

	private void replay()
	{
		EasyMock.replay(store, configuration);
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

	protected void startup() throws Exception
	{
		mockConfiguration(2);
		mockStore();
		replay();
		storage.startup(store, configuration);
	}

	protected void startupNonTransactional() throws Exception
	{
		mockStore();
		configuration = PowerMock.createStrictMock(HGConfiguration.class);
		EasyMock.expect(configuration.getHandleFactory()).andReturn(
				(HGHandleFactory) Class.forName(
						HGHANDLEFACTORY_IMPLEMENTATION_CLASS_NAME)
						.newInstance());
		EasyMock.expect(configuration.isTransactional()).andReturn(false)
				.times(2);
		replay();
		storage.startup(store, configuration);
	}

	protected void startup(final Exception whatToThrow) throws Exception
	{
		mockConfiguration(2);
		mockStore();
		EasyMock.expect(store.getTransactionManager()).andThrow(whatToThrow);
		replay();
		storage.startup(store, configuration);
	}

	protected void startup(final int callsBeforeExceptionIsThrown,
			final Exception whatToThrow) throws Exception
	{
		mockStore();
		configuration = PowerMock.createStrictMock(HGConfiguration.class);
		EasyMock.expect(configuration.getHandleFactory()).andReturn(
				(HGHandleFactory) Class.forName(
						HGHANDLEFACTORY_IMPLEMENTATION_CLASS_NAME)
						.newInstance());
		EasyMock.expect(configuration.isTransactional()).andReturn(true)
				.times(callsBeforeExceptionIsThrown);
		EasyMock.expect(configuration.isTransactional()).andThrow(whatToThrow);
		replay();
		storage.startup(store, configuration);
	}

	protected void startup(final int transactionManagerCalls) throws Exception
	{
		mockConfiguration(2);
		mockStore();
		// mock transaction manager
		final HGTransactionManager transactionManager = new HGTransactionManager(
				storage.getTransactionFactory());
		EasyMock.expect(store.getTransactionManager())
				.andReturn(transactionManager).times(transactionManagerCalls);
		replay();
		storage.startup(store, configuration);
	}

	protected void startupWithAdditionalTransaction(
			final int transactionManagerCalls) throws Exception
	{
		mockConfiguration(2);
		mockStore();
		// mock transaction manager
		final HGTransactionManager transactionManager = new HGTransactionManager(
				storage.getTransactionFactory());
		EasyMock.expect(store.getTransactionManager())
				.andReturn(transactionManager).times(transactionManagerCalls);
		replay();
		storage.startup(store, configuration);
		transactionManager.beginTransaction();
	}

	protected void shutdown() throws Exception
	{
		storage.shutdown();
	}

	/**
	 * Utility method. It can be used just for initialization set of handles in
	 * one line. Puts all given handles into hash set.
	 */
	public static Set<HGPersistentHandle> set(
			final HGPersistentHandle... handles)
	{
		final Set<HGPersistentHandle> allHandles = new HashSet<HGPersistentHandle>();
		for (final HGPersistentHandle eachHandle : handles)
		{
			allHandles.add(eachHandle);
		}
		return allHandles;
	}

	/**
	 * Utility method. Puts all handles which are accessible from given result
	 * set into hash set. In some test cases stored data returned as
	 * {@link HGRandomAccessResult}. Two results cannot be compared directly. So
	 * we put all handles into set and that compare two sets.
	 *
	 */
	// TODO investigate if the handles is ordered from one call to another and
	// return list of handles
	public static Set<HGPersistentHandle> set(
			final HGRandomAccessResult<HGPersistentHandle> handles)
	{
		final Set<HGPersistentHandle> allHandles = new HashSet<HGPersistentHandle>();
		while (handles.hasNext())
		{
			allHandles.add(handles.next());
		}
		return allHandles;
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