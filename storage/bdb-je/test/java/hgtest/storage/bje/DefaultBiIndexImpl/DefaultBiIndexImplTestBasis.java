package hgtest.storage.bje.DefaultBiIndexImpl;

import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;

import java.lang.reflect.Field;

import org.hypergraphdb.storage.bje.BJEConfig;
import org.hypergraphdb.storage.bje.DefaultBiIndexImpl;
import org.hypergraphdb.transaction.HGTransactionManager;

import com.sleepycat.je.Database;
import hgtest.storage.bje.IndexImplTestBasis;

public class DefaultBiIndexImplTestBasis extends IndexImplTestBasis
{
	protected static final String SECONDARY_DATABASE_FIELD_NAME = "secondaryDb";

	protected DefaultBiIndexImpl<Integer, String> indexImpl;

	/**
	 * Before environment can be closed all opened databases should be closed
	 * firstly. Links to these databases are stored in the fields of
	 * {@link org.hypergraphdb.storage.bje.DefaultBiIndexImpl}. We obtain them
	 * by field names. Such approach isn't good. But seems that there is not
	 * other way to obtain databases.
	 */
	protected void closeDatabases(final DefaultBiIndexImpl indexImpl)
			throws NoSuchFieldException, IllegalAccessException
	{
		// close database in DefaultIndexImpl
		final Field firstDatabaseField = indexImpl.getClass().getSuperclass()
				.getDeclaredField(FieldNames.DATABASE);
		firstDatabaseField.setAccessible(true);
		final Database firstDatabase = (Database) firstDatabaseField
				.get(indexImpl);
		// in some test cases first database is not opened, so don't close it
		if (firstDatabase != null)
		{
			firstDatabase.close();
		}

		// another database resides in DefaultBiIndexImpl
		final Field secondDatabaseField = indexImpl.getClass()
				.getDeclaredField(SECONDARY_DATABASE_FIELD_NAME);
		secondDatabaseField.setAccessible(true);
		final Database secondDatabase = ((Database) secondDatabaseField
				.get(indexImpl));
		// in some test cases second database is not opened, so don't close it
		if (secondDatabase != null)
		{
			secondDatabase.close();
		}
	}

	protected void mockStorage()
	{
		expect(mockedStorage.getConfiguration()).andReturn(new BJEConfig());
		expect(mockedStorage.getBerkleyEnvironment()).andReturn(environment)
				.times(3);
	}

	protected void startupIndex()
	{
		mockStorage();
		replay(mockedStorage);
		indexImpl = new DefaultBiIndexImpl(INDEX_NAME, mockedStorage,
				transactionManager, keyConverter, valueConverter, comparator,
				null);
		indexImpl.open();
	}

	protected void startupIndexWithFakeTransactionManager()
	{
		mockStorage();
		HGTransactionManager fakeTransactionManager = createStrictMock(HGTransactionManager.class);
		fakeTransactionManager.getContext();
		expectLastCall().andThrow(
				new IllegalStateException("Transaction manager is fake."));
		replay(mockedStorage, fakeTransactionManager);
		indexImpl = new DefaultBiIndexImpl(INDEX_NAME, mockedStorage,
				fakeTransactionManager, keyConverter, valueConverter,
				comparator, null);
		indexImpl.open();
	}
}
