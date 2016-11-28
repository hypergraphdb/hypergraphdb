package hgtest.storage.bje.DefaultBiIndexImpl;

import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;

import java.lang.reflect.Field;

import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.bje.DefaultBiIndexImpl;
import org.junit.Test;

import com.sleepycat.je.SecondaryDatabase;

public class DefaultBiIndexImpl_closeTest extends DefaultBiIndexImplTestBasis
{
	@Test
	public void doesNotFail_whenIndexIsNotOpenedAhead() throws Exception
	{
		replay(mockedStorage);

		final DefaultBiIndexImpl<Integer, String> indexImpl = new DefaultBiIndexImpl<>(
				INDEX_NAME, mockedStorage, transactionManager, keyConverter,
				valueConverter, comparator, null);

		indexImpl.close();
	}

	@Test
	public void happyPath() throws Exception
	{
		mockStorage();
		replay(mockedStorage);

		final DefaultBiIndexImpl<Integer, String> indexImpl = new DefaultBiIndexImpl<>(
				INDEX_NAME, mockedStorage, transactionManager, keyConverter,
				valueConverter, comparator, null);
		indexImpl.open();
		indexImpl.close();
	}

	@Test
	public void wrapsDatabaseException_withHypergraphException()
			throws Exception
	{
		final DefaultBiIndexImpl indexImpl = new TrickySetup()
				.openIndexNormally();
		new TrickySetup().useFakeSecondaryDatabase(indexImpl);

		try
		{
			below.expect(HGException.class);
			below.expectMessage("java.lang.IllegalStateException");
			indexImpl.close();
		}
		finally
		{
			closeDatabases(indexImpl);
		}
	}

	/**
	 * This class contains routines which are intended to test some corner cases
	 * (related to error handling).
	 */
	protected class TrickySetup
	{
		/**
		 * Returns well-formed index which is ready for any manipulations.
		 */
		protected DefaultBiIndexImpl openIndexNormally()
		{
			mockStorage();
			replay(mockedStorage);

			final DefaultBiIndexImpl<Integer, String> indexImpl = new DefaultBiIndexImpl<>(
					INDEX_NAME, mockedStorage, transactionManager,
					keyConverter, valueConverter, comparator, null);
			indexImpl.open();
			verify(mockedStorage);
			reset(mockedStorage);
			return indexImpl;
		}

		/**
		 * This method replaces original instance of Berkley database with fake
		 * one. Fake database will throw exception within
		 * {@link org.hypergraphdb.storage.bje.DefaultBiIndexImpl#open())
		 * method.
		 */
		protected void useFakeSecondaryDatabase(
				final DefaultBiIndexImpl indexImpl) throws Exception
		{
			// mock fake database
			final SecondaryDatabase fakeSecondaryDatabase = createStrictMock(SecondaryDatabase.class);
			fakeSecondaryDatabase.close();
			expectLastCall().andThrow(new IllegalStateException());
			replay(mockedStorage, fakeSecondaryDatabase);
			final Field secondaryDbField = indexImpl.getClass()
					.getDeclaredField(SECONDARY_DATABASE_FIELD_NAME);
			secondaryDbField.setAccessible(true);
			// close the real database before use fake one
			secondaryDbField.get(indexImpl).getClass().getMethod("close")
					.invoke(secondaryDbField.get(indexImpl));
			secondaryDbField.set(indexImpl, fakeSecondaryDatabase);
		}
	}
}
