package hgtest.storage.bdb.BDBStorageImplementation;

import hgtest.storage.bdb.NativeLibrariesWorkaround;
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
 *
 * @author Yuriy Sechko
 */

@PrepareForTest(HGConfiguration.class)
public class BDBStorageImplementationTestBasis extends PowerMockTestCase
{
    protected static final String HGHANDLEFACTORY_IMPLEMENTATION_CLASS_NAME = "org.hypergraphdb.handle.UUIDHandleFactory";

    // location of temporary directory for tests
    final File testDatabaseDirectory = hgtest.TestUtils.createTempFile(
            "BDBStorageImplementation", "test_database");
    final protected String testDatabaseLocation = hgtest.TestUtils
            .getCanonicalPath(testDatabaseDirectory);

    // classes which are used by BDBStorageImplementation
    HGStore store;
    HGConfiguration configuration;

    final BDBStorageImplementation storage = new BDBStorageImplementation();

    @BeforeMethod
    protected void resetMocksAndDeleteTestDirectory()
    {
        PowerMock.resetAll();
        hgtest.TestUtils.deleteDirectory(testDatabaseDirectory);
    }

    @AfterMethod
    protected void verifyMocksAndDeleteTestDirectory()
    {
        PowerMock.verifyAll();
        hgtest.TestUtils.deleteDirectory(testDatabaseDirectory);
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
        System.out.println(">>> What to throw: " + whatToThrow.getClass().getSuperclass());
        mockConfiguration(2);
        mockStore();
        EasyMock.expect(store.getTransactionManager()).andThrow(whatToThrow);
        replay();
        storage.startup(store, configuration);
    }

    /**
     *
     * @param callsBeforeExceptionIsThrown
     * @param whatToThrow
     * @throws Exception
     */
    protected void startup(final int callsBeforeExceptionIsThrown,
                           final Exception whatToThrow) throws Exception
    {
        NativeLibrariesWorkaround.loadNativeLibraries();
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

    protected void shutdown() throws Exception
    {
        storage.shutdown();
    }
}