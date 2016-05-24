package hgtest.storage.bje.DefaultIndexImpl;

import static hgtest.storage.bje.TestUtils.assertExceptions;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertNotNull;

import java.lang.reflect.Field;
import java.util.Comparator;

import org.easymock.EasyMock;
import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.bje.DefaultIndexImpl;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseNotFoundException;

/**
 * @author Yuriy Sechko
 */
public class DefaultIndexImpl_getComparatorTest extends
		DefaultIndexImplTestBasis
{
	@Test
	public void indexIsNotOpened() throws Exception
	{
		final Exception expected = new NullPointerException();

        replay(mockedStorage);
		final DefaultIndexImpl indexImpl = new DefaultIndexImpl(null, mockedStorage,
				transactionManager, keyConverter, valueConverter, comparator,
				null);

		try
		{
			indexImpl.getKeyComparator();
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

	@Test
	public void indexIsOpened() throws Exception
	{
		mockStorage();
        replay(mockedStorage);

		final DefaultIndexImpl indexImpl = new DefaultIndexImpl(INDEX_NAME,
                mockedStorage, transactionManager, keyConverter, valueConverter,
				comparator, null);
		indexImpl.open();

		final Comparator<byte[]> actualComparator = indexImpl
				.getKeyComparator();

		assertNotNull(actualComparator);
		closeDatabase(indexImpl);
	}

	@Test
	public void databaseThrowsException() throws Exception
	{
		mockStorage();
		Database fakeDatabase = PowerMock.createStrictMock(Database.class);
		EasyMock.expect(fakeDatabase.getConfig()).andThrow(
				new DatabaseNotFoundException(
						"This exception is thrown by fake database."));
        replay(mockedStorage, fakeDatabase);
		final DefaultIndexImpl indexImpl = new DefaultIndexImpl(INDEX_NAME,
                mockedStorage, transactionManager, keyConverter, valueConverter,
				comparator, null);
		indexImpl.open();

		// inject fake database and imitate error
		final Field databaseField = indexImpl.getClass().getDeclaredField(
				DATABASE_FIELD_NAME);
		databaseField.setAccessible(true);
		final Database realDatabase = (Database) databaseField.get(indexImpl);
		databaseField.set(indexImpl, fakeDatabase);

		// call method which is under test
		try
		{
			indexImpl.getKeyComparator();
		}
		catch (Exception ex)
		{
			assertExceptions(ex, HGException.class,
					"com.sleepycat.je.DatabaseNotFoundException",
					"This exception is thrown by fake database.");
		}
		finally
		{
			// set 'null' for reference to fake database
			// to make doubly sure that it will not be reused as mock
			fakeDatabase = null;
			// inject real database again
			databaseField.set(indexImpl, realDatabase);
			// close database like in other test cases
			closeDatabase(indexImpl);
		}
	}
}
