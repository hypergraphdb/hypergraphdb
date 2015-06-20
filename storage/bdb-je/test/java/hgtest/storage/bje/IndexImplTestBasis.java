package hgtest.storage.bje;

import com.google.code.multitester.annonations.Exported;
import com.sleepycat.je.*;
import org.easymock.EasyMock;
import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.storage.bje.BJEConfig;
import org.hypergraphdb.storage.bje.BJEStorageImplementation;
import org.hypergraphdb.storage.bje.DefaultIndexImpl;
import org.hypergraphdb.storage.bje.TransactionBJEImpl;
import org.hypergraphdb.transaction.*;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Comparator;

/**
 * Contains common code for test cases for
 * {@link org.hypergraphdb.storage.bje.DefaultIndexImpl} and
 * {@link org.hypergraphdb.storage.bje.DefaultBiIndexImpl} classes
 * 
 * @author Yuriy Sechko
 */
public class IndexImplTestBasis
{
	protected static final String INDEX_NAME = "sample_index";

	protected static final String DATABASE_FIELD_NAME = "db";
	protected static final String TRANSACTION_MANAGER_FIELD_NAME = "transactionManager";

	protected final File envHome = TestUtils.createTempFile("IndexImpl",
            "test_environment");

	// storage - used only for getting configuration data
	protected final BJEStorageImplementation storage = PowerMock
			.createStrictMock(BJEStorageImplementation.class);
	protected HGTransactionManager transactionManager;
	// custom converters
	protected ByteArrayConverter<Integer> keyConverter = new TestUtils.ByteArrayConverterForInteger();
	protected ByteArrayConverter<String> valueConverter = new TestUtils.ByteArrayConverterForString();

	// Use 'null' comparator - it forces
	// {@link org.hypergraphdb.storage.bje.DefaultIndexImpl} to use default
	// Sleepycat's BtreeComparator
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

	protected void startupEnvironment()
	{
		envHome.mkdir();
		config = new EnvironmentConfig();
		config.setAllowCreate(true).setReadOnly(false).setTransactional(true);
		environment = new Environment(envHome, config);
		transactionManager = new HGTransactionManager(
		// copied from the BJEStorageImplementation
				new HGTransactionFactory()
				{
					public HGStorageTransaction createTransaction(
							HGTransactionContext context,
							HGTransactionConfig config, HGTransaction parent)
					{
						try
						{
							TransactionConfig tconfig = new TransactionConfig();

							Durability tDurability = new Durability(
									Durability.SyncPolicy.WRITE_NO_SYNC,
									Durability.SyncPolicy.NO_SYNC, // unused
									// by
									// non-HA
									// applications.
									Durability.ReplicaAckPolicy.NONE); // unused
							// by
							// non-HA
							// applications.
							tconfig.setDurability(tDurability);

							Transaction tx = null;

							if (parent != null)
							{
								// Nested transaction are not supported by JE
								// Berkeley
								// DB.
								throw new IllegalStateException(
										"Nested transaction detected. Not supported by JE Berkeley DB.");
								// tx =
								// env.beginTransaction(((TransactionBJEImpl)parent.getStorageTransaction()).getBJETransaction(),
								// tconfig);
								// tx = env.beginTransaction(null, tconfig);
							}
							else
							{
								tx = environment
										.beginTransaction(null, tconfig);
							}
							return new TransactionBJEImpl(tx, environment);
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

					public boolean canRetryAfter(Throwable ex)
					{
						return ex instanceof TransactionConflictException
								|| ex instanceof LockConflictException; // DeadlockException;

					}
				});

	}

	// this method is used in most test cases for initializing fake instance of
	// BJEStorageImplementation
	protected void mockStorage()
	{
		EasyMock.expect(storage.getConfiguration()).andReturn(new BJEConfig());
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
			throws NoSuchFieldException, IllegalAccessException
	{
		// one database handle is in DefaultIndexImpl
		final Field firstDatabaseField = indexImpl.getClass().getDeclaredField(
				DATABASE_FIELD_NAME);
		firstDatabaseField.setAccessible(true);
		final Database firstDatabase = (Database) firstDatabaseField
				.get(indexImpl);
		// in some test cases first database is not opened, don't close them
		if (firstDatabase != null)
		{
			firstDatabase.close();
		}
	}
}
