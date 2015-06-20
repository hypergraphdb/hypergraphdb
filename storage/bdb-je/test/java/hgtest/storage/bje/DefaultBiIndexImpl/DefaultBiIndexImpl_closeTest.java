package hgtest.storage.bje.DefaultBiIndexImpl;

import com.sleepycat.je.SecondaryDatabase;
import org.easymock.EasyMock;
import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.bje.DefaultBiIndexImpl;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import java.lang.reflect.Field;

import static hgtest.storage.bje.TestUtils.assertExceptions;


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
		// open databases normally
		mockStorage();
		PowerMock.replayAll();
		final DefaultBiIndexImpl indexImpl = new DefaultBiIndexImpl(INDEX_NAME,
				storage, transactionManager, keyConverter, valueConverter,
				comparator);
		indexImpl.open();
		PowerMock.verifyAll();
		PowerMock.resetAll();
		// now we force to throw exception in the DefaultBiIndexImpl.close()
		// method
		// we link the field 'secondaryDb' to the fake database,
		// which throws exception when their 'close' method is called
		final SecondaryDatabase fakeSecondaryDatabase = PowerMock
				.createStrictMock(SecondaryDatabase.class);
		fakeSecondaryDatabase.close();
		EasyMock.expectLastCall().andThrow(new IllegalStateException());
		PowerMock.replayAll();
		final Field secondaryDbField = indexImpl.getClass().getDeclaredField(
				SECONDARY_DATABASE_FIELD_NAME);
		secondaryDbField.setAccessible(true);
		// close the real database before use fake
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
