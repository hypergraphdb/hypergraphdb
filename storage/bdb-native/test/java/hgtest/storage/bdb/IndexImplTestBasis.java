package hgtest.storage.bdb;

import com.google.code.multitester.annonations.Exported;
import com.sleepycat.db.*;
import org.easymock.EasyMock;
import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.storage.bdb.BDBConfig;
import org.hypergraphdb.storage.bdb.BDBStorageImplementation;
import org.hypergraphdb.storage.bdb.DefaultIndexImpl;
import org.hypergraphdb.storage.bdb.TransactionBDBImpl;
import org.hypergraphdb.transaction.*;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import java.io.File;
import java.io.FileNotFoundException;
import java.lang.reflect.Field;
import java.util.Comparator;

/**
 * Contains common code for test cases for
 * {@link org.hypergraphdb.storage.bdb.DefaultIndexImpl} and
 * {@link org.hypergraphdb.storage.bdb.DefaultBiIndexImpl} classes
 *
 * @author Yuriy Sechko
 */
public class IndexImplTestBasis
{
	// use workaround for loading native libraries
	static
	{
		NativeLibrariesWorkaround.loadNativeLibraries();
	}

	protected static final String INDEX_NAME = "sample_index";

	protected static final String DATABASE_FIELD_NAME = "db";
	protected static final String TRANSACTION_MANAGER_FIELD_NAME = "transactionManager";

	protected final File envHome = TestUtils.createTempFile("IndexImpl",
            "test_environment");

	protected final BDBStorageImplementation storage = PowerMock
			.createStrictMock(BDBStorageImplementation.class);
	protected HGTransactionManager transactionManager;
	protected ByteArrayConverter<Integer> keyConverter = new TestUtils.ByteArrayConverterForInteger();
	protected ByteArrayConverter<String> valueConverter = new TestUtils.ByteArrayConverterForString();

	protected Comparator<?> comparator = null;

	@BeforeMethod
	@Exported("up1")
	public void resetMocksAndDeleteTestDirectory() throws Exception
	{
		PowerMock.resetAll();
		TestUtils.deleteDirectory(envHome);
		startupEnvironment();
	}

	@AfterMethod
	@Exported("down1")
	public void verifyMocksAndDeleteTestDirectory() throws Exception
	{
		PowerMock.verifyAll();
		environment.close();
		TestUtils.deleteDirectory(envHome);
	}

	protected EnvironmentConfig config;
	protected Environment environment;

	protected void startupEnvironment() throws DatabaseException,
			FileNotFoundException
	{
		envHome.mkdir();
		config = new EnvironmentConfig();
		config.setAllowCreate(true);
		config.setTransactional(true);
		config.setInitializeCache(true);
		environment = new Environment(envHome, config);
		transactionManager = new HGTransactionManager(
		// copied from the BDBStorageImplementation
				new HGTransactionFactory()
				{
					public HGStorageTransaction createTransaction(
							HGTransactionContext context,
							HGTransactionConfig config, HGTransaction parent)
					{
						try
						{
							TransactionConfig tconfig = new TransactionConfig();
							if (environment.getConfig().getMultiversion()
									&& config.isReadonly())
								tconfig.setSnapshot(true);
							tconfig.setWriteNoSync(true);
							// tconfig.setNoSync(true);
							Transaction tx = null;
							if (parent != null)
								tx = environment.beginTransaction(
										((TransactionBDBImpl) parent
												.getStorageTransaction())
												.getBDBTransaction(), tconfig);
							else
								tx = environment
										.beginTransaction(null, tconfig);
							return new TransactionBDBImpl(tx, environment);
						}
						catch (DatabaseException ex)
						{
							// System.err.println("Failed to create transaction, will exit - temporary behavior to be removed at some point.");
							ex.printStackTrace(System.err);
							// System.exit(-1);
							throw new HGException(
									"Failed to create BerkeleyDB transaction object.",
									ex);
						}
					}

					public boolean canRetryAfter(Throwable t)
					{
						return t instanceof TransactionConflictException
								|| t instanceof DeadlockException;
					}
				});
	}

	// this method is used in most test cases for initializing fake instance of
	// BDBStorageImplementation
	protected void mockStorage()
	{
		EasyMock.expect(storage.getConfiguration()).andReturn(new BDBConfig());
		EasyMock.expect(storage.getBerkleyEnvironment()).andReturn(environment)
				.times(1);
		EasyMock.expect(storage.getConfiguration()).andReturn(new BDBConfig());
		EasyMock.expect(storage.getBerkleyEnvironment()).andReturn(environment)
				.times(1);
	}

	/**
	 * Before environment can be closed all opened databases should be closed
	 * first. Links to these databases stored in fields of DefaultBiIndexImpl.
	 * We obtain them by their names. It is not good. But it seems that there is
	 * not way to obtain them from Environment instance.
	 */
	protected void closeDatabase(final DefaultIndexImpl indexImpl)
			throws NoSuchFieldException, IllegalAccessException,
			DatabaseException
	{
		final Field firstDatabaseField = indexImpl.getClass().getDeclaredField(
				DATABASE_FIELD_NAME);
		firstDatabaseField.setAccessible(true);
		final Database firstDatabase = (Database) firstDatabaseField
				.get(indexImpl);
		if (firstDatabase != null)
		{
			firstDatabase.close();
		}
	}
}
