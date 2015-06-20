package hgtest.storage.bje.DefaultIndexImpl;

import com.sleepycat.je.Database;
import org.easymock.EasyMock;
import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.bje.DefaultIndexImpl;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import java.lang.reflect.Field;

import static hgtest.storage.bje.TestUtils.assertExceptions;


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
		// open databases normally
		mockStorage();
		PowerMock.replayAll();
		final DefaultIndexImpl indexImpl = new DefaultIndexImpl(INDEX_NAME,
				storage, transactionManager, keyConverter, valueConverter,
				comparator);
		indexImpl.open();
		PowerMock.verifyAll();
		PowerMock.resetAll();
		// now we force to throw exception in the DefaultIndexImpl.close()
		// method
		// we link the field 'db' to the fake database,
		// which throws exception when their 'close' method is called
		final Database fakeDatabase = PowerMock
				.createStrictMock(Database.class);
		fakeDatabase.close();
		EasyMock.expectLastCall().andThrow(new IllegalStateException());
		PowerMock.replayAll();
		final Field dbField = indexImpl.getClass().getDeclaredField(
				DATABASE_FIELD_NAME);
		dbField.setAccessible(true);
		// close the real database before use fake
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
