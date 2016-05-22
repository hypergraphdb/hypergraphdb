package hgtest.storage.bje.BJEStorageImplementation;

import static hgtest.storage.bje.TestUtils.*;
import static org.junit.rules.ExpectedException.none;

import java.io.File;

import org.easymock.EasyMock;
import org.hypergraphdb.HGConfiguration;
import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.HGStore;
import org.hypergraphdb.storage.bje.BJEStorageImplementation;
import org.hypergraphdb.transaction.HGTransactionManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.extension.listener.AnnotationEnabler;
import org.powermock.core.classloader.annotations.PowerMockListener;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * <p>
 * Most common actions which have to be performed in test cases for
 * {@link BJEStorageImplementation} (such as initialization, mocking, etc) are
 * defined in this class. <strong>PowerMock</strong> and
 * <strong>EasyMock</strong> frameworks are used. EasyMock's capabilities are
 * sufficient for mocking interfaces and plain classes. But some classes in
 * <strong>HypergraphDB</strong> modules are final. So we cannot mock them in
 * simple way. PowerMock allows to create mocks even for final classes. So we
 * can test {@link BJEStorageImplementation} in isolation from underlying
 * environment and (in the major number of cases) from other classes.
 * </p>
 * <p>
 * {@link BJEStorageImplementation} requires two parameters before it can be
 * utilized: {@link org.hypergraphdb.HGStore} and
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
 * Here we need to specify the handle factory and <code>isTransactional</code>
 * flag. In different test cases the number of calls may vary (for example, if
 * we test reading the data we firstly store it and then read it. Each time we
 * store/read {@link org.hypergraphdb.HGConfiguration#isTransactional()} is
 * called). So we specify how many times {@code isTransactional()} will be
 * called using parameter <code>calls</code>.
 * </p>
 * <p>
 * In some test cases we need to test behavior of
 * {@link BJEStorageImplementation} when tested method had not execute properly
 * and exception somewhere inside was thrown or transaction was not opened, etc.
 * For such test cases there are specific initialization methods (like
 * {@link hgtest.storage.bje.BJEStorageImplementation.BJEStorageImplementationTestBasis#startup(int, java.lang.Exception)}
 * and
 * {@link hgtest.storage.bje.BJEStorageImplementation.BJEStorageImplementationTestBasis#startupNonTransactional()
 * )}.
 * </p>
 */
@RunWith(PowerMockRunner.class)
@PowerMockListener(AnnotationEnabler.class)
@PrepareForTest(HGConfiguration.class)
public abstract class BJEStorageImplementationTestBasis
{
	protected static final String HGHANDLEFACTORY_IMPLEMENTATION_CLASS_NAME = "org.hypergraphdb.handle.UUIDHandleFactory";

	protected final File testDatabaseDirectory = createTempFile(
			"BJEStorageImplementation", "test_database");
	protected final String testDatabaseLocation = getCanonicalPath(testDatabaseDirectory);

	protected final HGStore store = PowerMock.createStrictMock(HGStore.class);

	protected final HGConfiguration configuration = PowerMock
			.createStrictMock(HGConfiguration.class);

	protected final BJEStorageImplementation storage = new BJEStorageImplementation();

	@Rule
	public final ExpectedException below = none();

	@Before
	public void resetMocksAndDeleteTestDirectory()
	{
		PowerMock.resetAll();
		deleteDirectory(testDatabaseDirectory);
	}

	@After
	public void verifyMocksAndDeleteTestDirectory()
	{
		PowerMock.verifyAll();
		deleteDirectory(testDatabaseDirectory);
	}

	private void replay()
	{
		EasyMock.replay(store, configuration);
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

	/**
	 * Used in most test cases (just because in most test cases
	 * {@link org.hypergraphdb.HGConfiguration#isTransactional()}) called two
	 * times)
	 */
	protected void startup() throws Exception
	{
		mockConfiguration(2);
		mockStore();
		replay();
		storage.startup(store, configuration);
	}

	protected void shutdown() throws Exception
	{
		storage.shutdown();
	}

	/**
	 * Used in test cases especially for checking <i>transaction manager's</i>
	 * behavior
	 */
	protected void startupNonTransactional() throws Exception
	{
		mockStore();
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
	 * Used in cases where specific exception is expected.
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
	 */
	protected void startup(final int callsBeforeExceptionIsThrown,
			final Exception whatToThrow) throws Exception
	{
		mockStore();
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
	 * Used in some test cases where <i>transaction manager</i> should be
	 * mocked,
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
}