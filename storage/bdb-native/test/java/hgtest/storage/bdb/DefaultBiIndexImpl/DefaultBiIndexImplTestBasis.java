package hgtest.storage.bdb.DefaultBiIndexImpl;

import com.sleepycat.db.Database;
import com.sleepycat.db.DatabaseException;
import hgtest.storage.bdb.DefaultIndexImpl.DefaultIndexImplTestBasis;
import org.easymock.EasyMock;
import org.hypergraphdb.storage.bdb.BDBConfig;
import org.hypergraphdb.storage.bdb.DefaultBiIndexImpl;
import org.hypergraphdb.transaction.HGTransactionManager;
import org.powermock.api.easymock.PowerMock;

import java.lang.reflect.Field;

/**
 * @author Yuriy Sechko
 */
public class DefaultBiIndexImplTestBasis extends DefaultIndexImplTestBasis
{
	protected static final String SECONDARY_DATABASE_FIELD_NAME = "secondaryDb";

	protected DefaultBiIndexImpl<Integer, String> indexImpl;

	protected void closeDatabases(final DefaultBiIndexImpl indexImpl)
			throws NoSuchFieldException, IllegalAccessException,
			DatabaseException
	{
		final Field firstDatabaseField = indexImpl.getClass().getSuperclass()
				.getDeclaredField(DATABASE_FIELD_NAME);
		firstDatabaseField.setAccessible(true);
		final Database firstDatabase = (Database) firstDatabaseField
				.get(indexImpl);
		if (firstDatabase != null)
		{
			firstDatabase.close();
		}
		final Field secondDatabaseField = indexImpl.getClass()
				.getDeclaredField(SECONDARY_DATABASE_FIELD_NAME);
		secondDatabaseField.setAccessible(true);
		final Database secondDatabase = ((Database) secondDatabaseField
				.get(indexImpl));
		if (secondDatabase != null)
		{
			secondDatabase.close();
		}
	}

	protected void mockStorage()
	{
		EasyMock.expect(storage.getConfiguration()).andReturn(new BDBConfig());
		EasyMock.expect(storage.getBerkleyEnvironment()).andReturn(environment);
		EasyMock.expect(storage.getConfiguration()).andReturn(new BDBConfig());
		EasyMock.expect(storage.getBerkleyEnvironment()).andReturn(environment)
				.times(2);
		EasyMock.expect(storage.getConfiguration()).andReturn(new BDBConfig())
				.times(2);
		EasyMock.expect(storage.getBerkleyEnvironment()).andReturn(environment);
	}

	protected void startupIndex()
	{
		mockStorage();
		PowerMock.replayAll();
		indexImpl = new DefaultBiIndexImpl(INDEX_NAME, storage,
				transactionManager, keyConverter, valueConverter, comparator);
		indexImpl.open();
	}

	protected void startupIndexWithFakeTransactionManager()
	{
		mockStorage();
		HGTransactionManager fakeTransactionManager = PowerMock
				.createStrictMock(HGTransactionManager.class);
		fakeTransactionManager.getContext();
		EasyMock.expectLastCall().andThrow(
				new IllegalStateException("Transaction manager is fake."));
		PowerMock.replayAll();
		indexImpl = new DefaultBiIndexImpl(INDEX_NAME, storage,
				fakeTransactionManager, keyConverter, valueConverter,
				comparator);
		indexImpl.open();
	}
}
