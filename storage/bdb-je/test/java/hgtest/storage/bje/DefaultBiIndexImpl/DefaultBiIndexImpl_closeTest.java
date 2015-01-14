package hgtest.storage.bje.DefaultBiIndexImpl;

import com.sleepycat.je.SecondaryDatabase;
import org.easymock.EasyMock;
import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.bje.DefaultBiIndexImpl;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import java.lang.reflect.Field;

import static org.testng.Assert.assertEquals;

/**
 * @author Yuriy Sechko
 */
public class DefaultBiIndexImpl_closeTest extends DefaultBiIndexImpl_TestBasis
{
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
	public void exceptionIsTrownOnClosingInternalDatabase() throws Exception
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
		// now we force to throw exception in the DefaultBiIndexImpl.close() method
		// we link the field 'secondaryDb' to the fake database,
		// which throws exception when their 'close' method is called
		final SecondaryDatabase fakeSecondaryDatabase = PowerMock
				.createStrictMock(SecondaryDatabase.class);
		fakeSecondaryDatabase.close();
		EasyMock.expectLastCall().andThrow(new IllegalStateException());
		PowerMock.replayAll();
		final Field secondaryDbField = indexImpl.getClass().getDeclaredField(
				"secondaryDb");
		secondaryDbField.setAccessible(true);
		// close the real database before use fake
		secondaryDbField.get(indexImpl).getClass().getMethod("close").invoke(secondaryDbField.get(indexImpl));
		secondaryDbField.set(indexImpl, fakeSecondaryDatabase);
		try
		{
			indexImpl.close();
		}
		catch (Exception occurred)
		{
			assertEquals(occurred.getClass(), expected.getClass());
			assertEquals(occurred.getMessage(), expected.getMessage());
		} finally {
			closeDatabases(indexImpl);
		}
	}
}
