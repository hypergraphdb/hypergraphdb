package hgtest.storage.bje.DefaultBiIndexImpl;

import com.sleepycat.je.*;
import hgtest.storage.bje.IndexImplTestBasis;
import org.easymock.EasyMock;
import org.hypergraphdb.storage.bje.BJEConfig;
import org.hypergraphdb.storage.bje.DefaultBiIndexImpl;
import org.hypergraphdb.transaction.*;
import org.powermock.api.easymock.PowerMock;
import java.lang.reflect.Field;

/**
 * @author Yuriy Sechko
 */
public class DefaultBiIndexImplTestBasis extends IndexImplTestBasis
{
	protected static final String SECONDARY_DATABASE_FIELD_NAME = "secondaryDb";

	protected DefaultBiIndexImpl<Integer, String> indexImpl;

	/**
	 * Before environment can be closed all opened databases should be closed
	 * first. Links to these databases stored in fields of DefaultBiIndexImpl.
	 * We obtain them by their names. It is not good. But it seems that there is
	 * not way to obtain them from Environment instance.
	 */
	protected void closeDatabases(final DefaultBiIndexImpl indexImpl)
			throws NoSuchFieldException, IllegalAccessException
	{
		// close database in DefaultIndexImpl
		final Field firstDatabaseField = indexImpl.getClass().getSuperclass()
				.getDeclaredField(DATABASE_FIELD_NAME);
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
				.getDeclaredField(SECONDARY_DATABASE_FIELD_NAME);
		secondDatabaseField.setAccessible(true);
		final Database secondDatabase = ((Database) secondDatabaseField
				.get(indexImpl));
		// in some test cases second database is not opened, don't close them
		if (secondDatabase != null)
		{
			secondDatabase.close();
		}
	}

	protected void mockStorage()
	{
		EasyMock.expect(storage.getConfiguration()).andReturn(new BJEConfig());
		EasyMock.expect(storage.getBerkleyEnvironment()).andReturn(environment)
				.times(3);
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
