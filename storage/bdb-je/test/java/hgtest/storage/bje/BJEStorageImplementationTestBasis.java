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
public class BJEStorageImplementationTestBasis extends PowerMockTestCase
{
	protected static final String HGHANDLEFACTORY_IMPLEMENTATION_CLASS_NAME = "org.hypergraphdb.handle.UUIDHandleFactory";

	// location of temporary directory for tests
	protected String testDatabaseLocation = System.getProperty("user.home")
			+ File.separator + "hgtest.tmp";

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

	protected void mockConfiguration(final int calls) throws Exception
	{
		configuration = PowerMock.createStrictMock(HGConfiguration.class);
		EasyMock.expect(configuration.getHandleFactory()).andReturn(
				(HGHandleFactory) Class.forName(
						HGHANDLEFACTORY_IMPLEMENTATION_CLASS_NAME)
						.newInstance());
		EasyMock.expect(configuration.isTransactional()).andReturn(true)
				.times(calls);
	}

	protected void mockStore() throws Exception
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

    protected void replay() {
        EasyMock.replay(store, configuration);
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

	protected void shutdown() throws Exception
	{
		storage.shutdown();
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