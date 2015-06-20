package hgtest.storage.bje.BJEStorageImplementation;

import com.google.code.multitester.annonations.Exported;
import hgtest.storage.bje.TestUtils;
import org.easymock.EasyMock;
import org.hypergraphdb.HGConfiguration;
import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.HGStore;
import org.hypergraphdb.storage.bje.BJEStorageImplementation;
import org.hypergraphdb.transaction.HGTransactionManager;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import java.io.File;

/**
 * Most common actions which have to be performed in test cases for
 * {@link BJEStorageImplementation} (such as initialization, mocking) are
 * extracted to this class. PowerMock + EasyMock frameworks are used. (see <a
 * href="http://code.google.com/p/powermock/">PowerMock page on Google Code</a>
 * and <a href="http://easymock.org/">EasyMock home page</a> for details).
 * EasyMock's capabilities are sufficient for interfaces and plain classes. But
 * some classes in hypergraphdb modules are final so we cannot mock them in
 * usual way. PowerMock allows to create mocks even for final classes. So we can
 * test {@link BJEStorageImplementation} in isolation from other environment and
 * in most cases from other classes (as far as possible).
 * <p>
 * {@link BJEStorageImplementation} requires two parameters before it can start
 * functioning: {@link org.hypergraphdb.HGStore} and
 * {@link org.hypergraphdb.HGConfiguration}. As mentioned above we test
 * {@link BJEStorageImplementation} in isolation. So the
 * {@link org.hypergraphdb.HGStore} and {@link org.hypergraphdb.HGConfiguration}
 * parameters should be mocked. It is done in
 * {@link BJEStorageImplementationTestBasis#mockStore()} and
 * {@link BJEStorageImplementationTestBasis#mockConfiguration(int)} methods.
 * <p>
 * The inner code in {@link BJEStorageImplementation} makes several calls to the
 * {@link org.hypergraphdb.HGStore} and {@link org.hypergraphdb.HGConfiguration}
 * objects. For {@link org.hypergraphdb.HGStore} only getDatabaseLocation() is
 * in use.
 * <p>
 * Mocking {@link org.hypergraphdb.HGConfiguration} object is more complicated.
 * Here we need to specify the handle factory and {@code isTransactional} flag.
 * In different test cases the number of calls may vary (For example if we test
 * reading the data we firstly store it and then read it. Each time we
 * store/read {@link org.hypergraphdb.HGConfiguration#isTransactional()} is
 * called). So we specify how many times {@code isTransactional()} will be
 * called using parameter {@code calls}.
 * <p>
 * In some test cases we need to test behavior of
 * {@link BJEStorageImplementation} when tested method had not execute properly
 * and exception somewhere inside was thrown or transaction was not opened etc.
 * For such test cases specific initialization methods (like
 * {@link hgtest.storage.bje.BJEStorageImplementation.BJEStorageImplementationTestBasis#startup(int, java.lang.Exception)}
 * and
 * {@link hgtest.storage.bje.BJEStorageImplementation.BJEStorageImplementationTestBasis#startupNonTransactional()
 * )} are convenient.
 * <p>
 * 
 * @author Yuriy Sechko
 */

@PrepareForTest(HGConfiguration.class)
public class BJEStorageImplementationTestBasis extends PowerMockTestCase
{
	protected static final String HGHANDLEFACTORY_IMPLEMENTATION_CLASS_NAME = "org.hypergraphdb.handle.UUIDHandleFactory";

	// location of temporary directory for tests
	final File testDatabaseDirectory = TestUtils.createTempFile(
            "BJEStorageImplementation", "test_database");
	final protected String testDatabaseLocation = TestUtils
			.getCanonicalPath(testDatabaseDirectory);

	// classes which are used by BJEStorageImplementation
	HGStore store;
	HGConfiguration configuration;

	@Exported("underTest")
	final BJEStorageImplementation storage = new BJEStorageImplementation();

	@BeforeMethod
	@Exported("up1")
	protected void resetMocksAndDeleteTestDirectory()
	{
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
	 * Used in cases where specific exception expected
	 * 
	 * @param whatToThrow
	 * @throws Exception
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
	 * On more variant of the
	 * {@link BJEStorageImplementationTestBasis#startup(java.lang.Exception)}.
	 * The difference is only in number of successful calls
	 * {@link org.hypergraphdb.HGConfiguration#isTransactional()} before
	 * exception occurs.
	 * 
	 * @param callsBeforeExceptionIsThrown
	 * @param whatToThrow
	 * @throws Exception
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
	 * Used in some test cases where transaction manager should be mocked,
	 * 
	 * @param transactionManagerCalls
	 * @throws Exception
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
	 * Used in test cases for incidence links
	 * 
	 * @param transactionManagerCalls
	 * @throws Exception
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