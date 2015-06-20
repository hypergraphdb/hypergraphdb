package hgtest.storage.bje.DefaultIndexImpl;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseNotFoundException;
import org.easymock.EasyMock;
import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.bje.DefaultIndexImpl;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.Comparator;

import static hgtest.storage.bje.TestUtils.assertExceptions;
import static org.testng.Assert.assertNull;

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

		PowerMock.replayAll();
		final DefaultIndexImpl indexImpl = new DefaultIndexImpl(null, storage,
				transactionManager, keyConverter, valueConverter, comparator);

		try
		{
			indexImpl.getComparator();
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
		PowerMock.replayAll();
		final DefaultIndexImpl indexImpl = new DefaultIndexImpl(INDEX_NAME,
				storage, transactionManager, keyConverter, valueConverter,
				comparator);
		indexImpl.open();

		final Comparator<byte[]> actualComparator = indexImpl.getComparator();

		assertNull(actualComparator);
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
		PowerMock.replayAll();
		final DefaultIndexImpl indexImpl = new DefaultIndexImpl(INDEX_NAME,
				storage, transactionManager, keyConverter, valueConverter,
				comparator);
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
			indexImpl.getComparator();
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
