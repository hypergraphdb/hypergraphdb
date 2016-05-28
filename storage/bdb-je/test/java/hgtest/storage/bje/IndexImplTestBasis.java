package hgtest.storage.bje;

import static com.sleepycat.je.Durability.ReplicaAckPolicy;
import static com.sleepycat.je.Durability.SyncPolicy;
import static hgtest.storage.bje.TestUtils.deleteDirectory;
import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.junit.rules.ExpectedException.none;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Comparator;

import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.storage.bje.BJEConfig;
import org.hypergraphdb.storage.bje.BJEStorageImplementation;
import org.hypergraphdb.storage.bje.DefaultIndexImpl;
import org.hypergraphdb.storage.bje.TransactionBJEImpl;
import org.hypergraphdb.transaction.HGStorageTransaction;
import org.hypergraphdb.transaction.HGTransaction;
import org.hypergraphdb.transaction.HGTransactionConfig;
import org.hypergraphdb.transaction.HGTransactionContext;
import org.hypergraphdb.transaction.HGTransactionFactory;
import org.hypergraphdb.transaction.HGTransactionManager;
import org.hypergraphdb.transaction.TransactionConflictException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;

/**
 * Contains common code of test cases for
 * {@link org.hypergraphdb.storage.bje.DefaultIndexImpl} and
 * {@link org.hypergraphdb.storage.bje.DefaultBiIndexImpl}.
 */
public class IndexImplTestBasis
{
	protected static final String INDEX_NAME = "sample_index";

	protected static class FieldNames
	{
		public static final String DATABASE = "db";
		public static final String TRANSACTION_MANAGER = "transactionManager";

	}

	protected final File envHome = TestUtils.createTempFile("IndexImpl",
			"test_environment");

	/**
	 * Mock of storage is used only for getting configuration data.
	 */
	protected final BJEStorageImplementation mockedStorage = createStrictMock(BJEStorageImplementation.class);

	protected HGTransactionManager transactionManager;

	protected ByteArrayConverter<Integer> keyConverter = new TestUtils.ByteArrayConverterForInteger();
	protected ByteArrayConverter<String> valueConverter = new TestUtils.ByteArrayConverterForString();

	/**
	 * Use <code>null</code> comparator: it forces
	 * {@link org.hypergraphdb.storage.bje.DefaultIndexImpl} to use default
	 * Sleepycat's BtreeComparator.
	 */
	protected Comparator<byte[]> comparator = null;

	@Rule
	public final ExpectedException below = none();

	@Before
	public void resetMocksAndDeleteTestDirectory() throws Exception
	{
		reset(mockedStorage);
		deleteDirectory(envHome);
		startupEnvironment();
	}

	@After
	public void verifyMocksAndDeleteTestDirectory() throws Exception
	{
		verify(mockedStorage);
		environment.close();
		deleteDirectory(envHome);
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
		// copied and pasted from the BJEStorageImplementation and slightly
		// simplified for the sake of brevity
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
									SyncPolicy.WRITE_NO_SYNC,
									SyncPolicy.NO_SYNC, ReplicaAckPolicy.NONE);
							tconfig.setDurability(tDurability);

							Transaction tx;

							if (parent != null)
							{
								throw new IllegalStateException(
										"Nested transaction detected. Not supported by JE Berkeley DB.");
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
							ex.printStackTrace(System.err);
							throw new HGException(
									"Failed to create BerkeleyDB transaction object.",
									ex);
						}
					}

					public boolean canRetryAfter(Throwable ex)
					{
						return ex instanceof TransactionConflictException
								|| ex instanceof LockConflictException;

					}
				});

	}

	/**
	 * This method is used in most test cases for initializing fake instance of
	 * {@link org.hypergraphdb.storage.bje.BJEStorageImplementation}.
	 */
	protected void mockStorage()
	{
		expect(mockedStorage.getConfiguration()).andReturn(new BJEConfig());
		expect(mockedStorage.getBerkleyEnvironment()).andReturn(environment)
				.times(1);
	}

	/**
	 * Before environment can be closed all opened databases should be closed
	 * first. Links to these databases stored in fields of DefaultBiIndexImpl.
	 * We obtain them by their names. It is not good. But it seems that there is
	 * not way to obtain them from Environment instance.
	 */
	protected void closeDatabase(final DefaultIndexImpl<?, ?> indexImpl)
			throws NoSuchFieldException, IllegalAccessException
	{
		// one database handle resides in DefaultIndexImpl
		final Field firstDatabaseField = indexImpl.getClass().getDeclaredField(
				FieldNames.DATABASE);
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
