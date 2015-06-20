package hgtest.storage.bdb.BDBStorageImplementation;

import com.google.code.multitester.annonations.Exported;
import hgtest.storage.bdb.NativeLibrariesWorkaround;
import hgtest.storage.bdb.TestUtils;
import org.easymock.EasyMock;
import org.hypergraphdb.HGConfiguration;
import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.HGStore;
import org.hypergraphdb.storage.bdb.BDBStorageImplementation;
import org.hypergraphdb.transaction.HGTransactionManager;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import java.io.File;

/**
 * Base class for test cases for
 * {@link org.hypergraphdb.storage.bdb.BDBStorageImplementation}. Very similar
 * to BJEStorageImplementationTestBasis.
 * <p>
 * PowerMock + EasyMock are used. Path to native libraries is established by
 * Maven. Native libraries have to be loaded in
 * {@link hgtest.storage.bdb.BDBStorageImplementation.BDBStorageImplementationTestBasis#startup()}
 * method.
 * 
 * @author Yuriy Sechko
 */

@PrepareForTest(HGConfiguration.class)
public class BDBStorageImplementationTestBasis extends PowerMockTestCase
{
	protected static final String HGHANDLEFACTORY_IMPLEMENTATION_CLASS_NAME = "org.hypergraphdb.handle.UUIDHandleFactory";

	// location of temporary directory for tests
	final File testDatabaseDirectory = TestUtils.createTempFile(
            "BDBStorageImplementation", "test_database");
	final protected String testDatabaseLocation = TestUtils
			.getCanonicalPath(testDatabaseDirectory);

	// classes which are used by BDBStorageImplementation (used by
	// BJEStorageImplementation as well)
	HGStore store;
	HGConfiguration configuration;

	// instance of storage which is under test
	@Exported("underTest")
	final BDBStorageImplementation storage = new BDBStorageImplementation();

	@BeforeMethod
	@Exported("up1")
	protected void resetMocksAndDeleteTestDirectory()
	{
		NativeLibrariesWorkaround.loadNativeLibraries();
		PowerMock.resetAll();
		TestUtils.deleteDirectory(testDatabaseDirectory);
	}

	@AfterMethod
	@Exported("down1")
	protected void verifyMocksAndDeleteTestDirectory()
	{
		PowerMock.verifyAll();
		TestUtils.deleteDirectory(testDatabaseDirectory);
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

	/**
	 * Used in most test cases (just because in most test cases
	 * {@link org.hypergraphdb.HGConfiguration#isTransactional()}) called two
	 * times)
	 *
	 * @throws Exception
	 */
	protected void startup() throws Exception
	{
		mockConfiguration(2);
		mockStore();
		replay();
		storage.startup(store, configuration);
	}

	/**
	 * Used in test cases especially for checking transaction manager's behavior
	 *
	 * @throws Exception
	 */
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

	/**
	 * Used in cases where specific exception expected.
	 *
	 * @param whatToThrow
	 *            exception that will be thrown by mock (making fake error)
	 */
	protected void startup(final Exception whatToThrow) throws Exception
	{
		mockConfiguration(2);
		mockStore();
		EasyMock.expect(store.getTransactionManager()).andThrow(whatToThrow);
		replay();
		storage.startup(store, configuration);
	}

	/**
	 * Almost the same as in BJEStorageImplementation.
	 */
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

	/**
	 * Used in some test cases where transaction manager should be mocked.
	 */
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

	/**
	 * Used in test cases for incidence links.
	 */
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

	/**
	 * Shortcuts for use in common test cases.
	 */
	@Exported("up_0")
	protected void startup_0() throws Exception
	{
		startup();
	}

	@Exported("up_1")
	protected void startup_1() throws Exception
	{
		startup(1);
	}

	@Exported("up_2")
	protected void startup_2() throws Exception
	{
		startup(2);
	}

	@Exported("up_3")
	protected void startup_3() throws Exception
	{
		startup(3);
	}

	@Exported("up_4")
	protected void startup_4() throws Exception
	{
		startup(4);
	}

	@Exported("up_t_3")
	protected void startup_transaction_3() throws Exception
	{
		startupWithAdditionalTransaction(3);
	}

	@Exported("up_t_5")
	protected void startup_transaction_5() throws Exception
	{
		startupWithAdditionalTransaction(5);
	}

	@Exported("down2")
	protected void shutdown() throws Exception
	{
		storage.shutdown();
	}
}