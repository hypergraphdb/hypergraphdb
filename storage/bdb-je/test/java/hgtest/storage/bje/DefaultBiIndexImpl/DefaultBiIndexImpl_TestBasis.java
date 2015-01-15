package hgtest.storage.bje.DefaultBiIndexImpl;

import com.sleepycat.je.*;
import org.easymock.EasyMock;
import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.BAUtils;
import org.hypergraphdb.storage.BAtoString;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.storage.bje.BJEConfig;
import org.hypergraphdb.storage.bje.BJEStorageImplementation;
import org.hypergraphdb.storage.bje.DefaultBiIndexImpl;
import org.hypergraphdb.storage.bje.TransactionBJEImpl;
import org.hypergraphdb.transaction.*;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Comparator;

import static hgtest.storage.bje.TestUtils.deleteDirectory;

/**
 * @author Yuriy Sechko
 */
public class DefaultBiIndexImpl_TestBasis
{
	protected final File envHome = new File(System.getProperty("user.home")
			+ File.separator + "test_environment");

	protected static final String INDEX_NAME = "sample_index";
	// storage - used only for getting configuration data
	protected final BJEStorageImplementation storage = PowerMock
			.createStrictMock(BJEStorageImplementation.class);
	protected HGTransactionManager transactionManager;
    // custom converters
	protected ByteArrayConverter<Integer> keyConverter = new ByteArrayConverter<Integer>()
	{
		public byte[] toByteArray(final Integer input)
		{
			final byte[] buffer = new byte[4];
			BAUtils.writeInt(input, buffer, 0);
			return buffer;
		}

		public Integer fromByteArray(final byte[] byteArray, final int offset,
				final int length)
		{
			return BAUtils.readInt(byteArray, 0);
		}
	};

	protected ByteArrayConverter<String> valueConverter = new ByteArrayConverter<String>()
	{
		public byte[] toByteArray(final String input)
		{
			return BAtoString.getInstance().toByteArray(input);
		}

		public String fromByteArray(final byte[] byteArray, final int offset,
				final int length)
		{
			return BAtoString.getInstance().fromByteArray(byteArray, offset,
					length);
		}
	};

	protected Comparator<?> comparator = PowerMock
			.createStrictMock(Comparator.class);

	// real instances we need: Database, Environment (come from the
	// Sleepycat je library)
	protected EnvironmentConfig config;
	protected Environment environment;

	@BeforeMethod
	protected void resetMocksAndDeleteTestDirectory()
	{
		PowerMock.resetAll();
		deleteDirectory(envHome);
		startupEnvironment();
	}

	@AfterMethod
	protected void verifyMocksAndDeleteTestDirectory()
	{
		PowerMock.verifyAll();
		shutdownEnvironment();
		deleteDirectory(envHome);
	}

	protected void startupEnvironment()
	{
		envHome.mkdirs();
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

	protected void shutdownEnvironment()
	{
		environment.close();
	}

	/**
	 * Before environment can be closed all opened databases should be closed
	 * first. Links to these databases stored in fields of DefaultBiIndexImpl.
	 * We obtain them by their names. It is not good. But it seems that there is
	 * not way to obtain them from Environment instance.
	 */
	protected void closeDatabases(final DefaultBiIndexImpl indexImpl)
			throws NoSuchFieldException, IllegalAccessException
	{
		// one database handle is in DefaultIndexImpl
		final Field firstDatabaseField = indexImpl.getClass().getSuperclass()
				.getDeclaredField("db");
		firstDatabaseField.setAccessible(true);
		final Database firstDatabase = (Database) firstDatabaseField
				.get(indexImpl);
		// in some test cases first database is not opened, don't close them
		if (firstDatabase != null)
		{
			firstDatabase.close();
		}
		// another is in DefaultBiIndexImpl
		final Field secondDatabaseField = indexImpl.getClass()
				.getDeclaredField("secondaryDb");
		secondDatabaseField.setAccessible(true);
		final Database secondDatabase = ((Database) secondDatabaseField
				.get(indexImpl));
		// in some test cases second database is not opened, don't close them
		if (secondDatabase != null)
		{
			secondDatabase.close();
		}
	}

	// this method is used in most test cases for initializing fake instance of
	// BJEStorageImplementation
	protected void mockStorage()
	{
		EasyMock.expect(storage.getConfiguration()).andReturn(new BJEConfig());
		EasyMock.expect(storage.getBerkleyEnvironment()).andReturn(environment)
				.times(3);
	}
}
