package hgtest.storage.bje.DefaultBiIndexImpl;

import com.sleepycat.je.SecondaryDatabase;
import org.easymock.EasyMock;
import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.bje.DefaultBiIndexImpl;
import org.powermock.api.easymock.PowerMock;
import org.junit.Test;

import java.lang.reflect.Field;

import static hgtest.storage.bje.TestUtils.assertExceptions;
import static org.easymock.EasyMock.*;

public class DefaultBiIndexImpl_closeTest extends DefaultBiIndexImplTestBasis
{
	@Test
	public void indexIsNotOpened() throws Exception
	{
		replay(mockedStorage);

		final DefaultBiIndexImpl indexImpl = new DefaultBiIndexImpl(INDEX_NAME,
				mockedStorage, transactionManager, keyConverter,
				valueConverter, comparator, null);

		indexImpl.close();
	}

	@Test
	public void allInternalOperationsPerformFine() throws Exception
	{
		mockStorage();
        replay(mockedStorage);

		final DefaultBiIndexImpl indexImpl = new DefaultBiIndexImpl(INDEX_NAME,
				mockedStorage, transactionManager, keyConverter,
				valueConverter, comparator, null);
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
        replay(mockedStorage);

		final DefaultBiIndexImpl indexImpl = new DefaultBiIndexImpl(INDEX_NAME,
				mockedStorage, transactionManager, keyConverter,
				valueConverter, comparator, null);
		indexImpl.open();
		verify(mockedStorage);
		reset(mockedStorage);
		// now we force to throw exception in the DefaultBiIndexImpl.close()
		// method
		// we link the field 'secondaryDb' to the fake database,
		// which throws exception when their 'close' method is called
		final SecondaryDatabase fakeSecondaryDatabase = createStrictMock(SecondaryDatabase.class);
		fakeSecondaryDatabase.close();
		expectLastCall().andThrow(new IllegalStateException());
		replay(mockedStorage, fakeSecondaryDatabase);
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
