package hgtest.storage.bdb.DefaultIndexImpl;

import com.sleepycat.db.Database;
import org.easymock.EasyMock;
import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.bdb.DefaultIndexImpl;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import java.lang.reflect.Field;

import static hgtest.storage.bdb.TestUtils.assertExceptions;


/**
 * @author Yuriy Sechko
 */
public class DefaultIndexImpl_closeTest extends DefaultIndexImplTestBasis
{
	@Test
	public void indexIsNotOpenedYet() throws Exception
	{
		PowerMock.replayAll();

		final DefaultIndexImpl indexImpl = new DefaultIndexImpl(INDEX_NAME,
				storage, transactionManager, keyConverter, valueConverter,
				comparator);

		indexImpl.close();
	}

	@Test
	public void allInternalOperationsPerformFine() throws Exception
	{
		mockStorage();
		PowerMock.replayAll();

		final DefaultIndexImpl indexImpl = new DefaultIndexImpl(INDEX_NAME,
				storage, transactionManager, keyConverter, valueConverter,
				comparator);
		indexImpl.open();

		indexImpl.close();
	}

	@Test
	public void exceptionIsThrownOnClosingInternalDatabase() throws Exception
	{
		final HGException expected = new HGException(
				"java.lang.IllegalStateException");
		mockStorage();
		PowerMock.replayAll();
		final DefaultIndexImpl indexImpl = new DefaultIndexImpl(INDEX_NAME,
				storage, transactionManager, keyConverter, valueConverter,
				comparator);
		indexImpl.open();
		PowerMock.verifyAll();
		PowerMock.resetAll();
		final Database fakeDatabase = PowerMock
				.createStrictMock(Database.class);
		fakeDatabase.close();
		EasyMock.expectLastCall().andThrow(new IllegalStateException());
		PowerMock.replayAll();
		final Field dbField = indexImpl.getClass().getDeclaredField(
				DATABASE_FIELD_NAME);
		dbField.setAccessible(true);
		dbField.get(indexImpl).getClass().getMethod("close")
				.invoke(dbField.get(indexImpl));
		dbField.set(indexImpl, fakeDatabase);
		try
		{
			indexImpl.close();
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
		}
		finally
		{
			closeDatabase(indexImpl);
		}
	}
}
