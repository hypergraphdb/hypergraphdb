package hgtest.storage.bje.DefaultIndexImpl;

import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.hamcrest.core.AllOf.allOf;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertNotNull;

import java.lang.reflect.Field;
import java.util.Comparator;

import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.bje.DefaultIndexImpl;
import org.junit.Test;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseNotFoundException;

public class DefaultIndexImpl_getComparatorTest extends
		DefaultIndexImplTestBasis
{
	@Test
	public void throwsException_whenIndexIsNotOpenedAhead() throws Exception
	{
		replay(mockedStorage);
		final DefaultIndexImpl<Integer, String> indexImpl = new DefaultIndexImpl<>(
				null, mockedStorage, transactionManager, keyConverter,
				valueConverter, comparator, null);

		try
		{
			below.expect(NullPointerException.class);
			indexImpl.getKeyComparator();
		}
		finally
		{
			closeDatabase(indexImpl);
		}
	}

	@Test
	public void happyPath() throws Exception
	{
		mockStorage();
		replay(mockedStorage);

		final DefaultIndexImpl<Integer, String> indexImpl = new DefaultIndexImpl<>(
				INDEX_NAME, mockedStorage, transactionManager, keyConverter,
				valueConverter, comparator, null);
		indexImpl.open();

		final Comparator<byte[]> actualComparator = indexImpl
				.getKeyComparator();
		assertNotNull(actualComparator);

		closeDatabase(indexImpl);
	}

	@Test
	public void wrapsUnderlyingException_withHypergraphException()
			throws Exception
	{
		mockStorage();
		Database fakeDatabase = createStrictMock(Database.class);
		expect(fakeDatabase.getConfig()).andThrow(
				new DatabaseNotFoundException(
						"This exception is thrown by fake database."));
		replay(mockedStorage, fakeDatabase);
		final DefaultIndexImpl<Integer, String> indexImpl = new DefaultIndexImpl<>(
				INDEX_NAME, mockedStorage, transactionManager, keyConverter,
				valueConverter, comparator, null);
		indexImpl.open();

		// inject fake database and imitate error
		final Field databaseField = indexImpl.getClass().getDeclaredField(
				FieldNames.DATABASE);
		databaseField.setAccessible(true);
		final Database realDatabase = (Database) databaseField.get(indexImpl);
		databaseField.set(indexImpl, fakeDatabase);

		// call method which is under test
		try
		{
			below.expect(HGException.class);
			below.expectMessage(allOf(
					containsString("com.sleepycat.je.DatabaseNotFoundException"),
					containsString("This exception is thrown by fake database.")));
			indexImpl.getKeyComparator();
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
