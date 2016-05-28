package hgtest.storage.bje.DefaultIndexImpl;

import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;

import java.lang.reflect.Field;

import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.bje.DefaultIndexImpl;
import org.junit.Test;

import com.sleepycat.je.Database;

public class DefaultIndexImpl_closeTest extends DefaultIndexImplTestBasis
{
	@Test
	public void doesNotFails_whenIndexIsNotOpenedAhead() throws Exception
	{
		replay(mockedStorage);

		final DefaultIndexImpl<Integer, String> indexImpl = new DefaultIndexImpl<>(INDEX_NAME,
				mockedStorage, transactionManager, keyConverter,
				valueConverter, comparator, null);

		indexImpl.close();
	}

	@Test
	public void happyPath() throws Exception
	{
		mockStorage();
		replay(mockedStorage);

		final DefaultIndexImpl<Integer, String> indexImpl = new DefaultIndexImpl<>(INDEX_NAME,
				mockedStorage, transactionManager, keyConverter,
				valueConverter, comparator, null);
		indexImpl.open();

		indexImpl.close();
	}

	@Test
	public void wrapsUnderlyingException_withHypergraphException()
			throws Exception
	{
		final DefaultIndexImpl indexImpl = new TrickySetup()
				.openIndexNormally();
		new TrickySetup().useFakeDatabase(indexImpl);

		try
		{
			below.expect(HGException.class);
			below.expectMessage("java.lang.IllegalStateException");
			indexImpl.close();
		}
		finally
		{
			closeDatabase(indexImpl);
		}
	}

	protected class TrickySetup
	{
		protected DefaultIndexImpl openIndexNormally()
		{
			mockStorage();
			replay(mockedStorage);
			final DefaultIndexImpl<Integer, String> indexImpl = new DefaultIndexImpl<>(INDEX_NAME,
					mockedStorage, transactionManager, keyConverter,
					valueConverter, comparator, null);
			indexImpl.open();
			verify(mockedStorage);
			reset(mockedStorage);
			return indexImpl;
		}

		/**
		 * Here we force to throw exception in the DefaultIndexImpl.close()
		 * method. We link the field 'db' to the fake database, which throws
		 * exception when their 'close' method is called.
		 */
		protected void useFakeDatabase(DefaultIndexImpl indexImpl)
				throws Exception
		{
			final Database fakeDatabase = createStrictMock(Database.class);
			fakeDatabase.close();
			expectLastCall().andThrow(new IllegalStateException());
			replay(mockedStorage, fakeDatabase);
			final Field dbField = indexImpl.getClass().getDeclaredField(
                    FieldNames.DATABASE);
			dbField.setAccessible(true);
			// close real database before use fake one
			dbField.get(indexImpl).getClass().getMethod("close")
					.invoke(dbField.get(indexImpl));
			dbField.set(indexImpl, fakeDatabase);
		}
	}
}
