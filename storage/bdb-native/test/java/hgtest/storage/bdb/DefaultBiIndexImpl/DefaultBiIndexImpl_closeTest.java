package hgtest.storage.bdb.DefaultBiIndexImpl;

import com.sleepycat.db.SecondaryDatabase;
import org.easymock.EasyMock;
import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.bdb.DefaultBiIndexImpl;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import java.lang.reflect.Field;

import static hgtest.storage.bdb.TestUtils.assertExceptions;


/**
 * @author Yuriy Sechko
 */
public class DefaultBiIndexImpl_closeTest extends DefaultBiIndexImplTestBasis
{
	@Test
	public void indexIsNotOpened() throws Exception
	{
		PowerMock.replayAll();

		final DefaultBiIndexImpl indexImpl = new DefaultBiIndexImpl(INDEX_NAME,
				storage, transactionManager, keyConverter, valueConverter,
				comparator);

		indexImpl.close();
	}

	@Test
	public void allInternalOperationsPerformFine() throws Exception
	{
		mockStorage();
		PowerMock.replayAll();
		final DefaultBiIndexImpl indexImpl = new DefaultBiIndexImpl(INDEX_NAME,
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
		final DefaultBiIndexImpl indexImpl = new DefaultBiIndexImpl(INDEX_NAME,
				storage, transactionManager, keyConverter, valueConverter,
				comparator);
		indexImpl.open();
		PowerMock.verifyAll();
		PowerMock.resetAll();
		final SecondaryDatabase fakeSecondaryDatabase = PowerMock
				.createStrictMock(SecondaryDatabase.class);
		fakeSecondaryDatabase.close();
		EasyMock.expectLastCall().andThrow(new IllegalStateException());
		PowerMock.replayAll();
		final Field secondaryDbField = indexImpl.getClass().getDeclaredField(
				SECONDARY_DATABASE_FIELD_NAME);
		secondaryDbField.setAccessible(true);
		secondaryDbField.get(indexImpl).getClass().getMethod("close")
				.invoke(secondaryDbField.get(indexImpl));
		secondaryDbField.set(indexImpl, fakeSecondaryDatabase);
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
			closeDatabases(indexImpl);
		}
	}
}
