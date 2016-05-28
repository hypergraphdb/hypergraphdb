package hgtest.storage.bje.DefaultIndexImpl;

import static org.easymock.EasyMock.*;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertThat;

import java.lang.reflect.Field;

import org.hamcrest.CoreMatchers;
import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.bje.DefaultIndexImpl;
import org.junit.Test;

import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseNotFoundException;

public class DefaultIndexImpl_countTest extends DefaultIndexImplTestBasis
{
	@Test
	public void throwsException_whenIndexIsNotOpenedAhead() throws Exception
	{
		replay(mockedStorage);
		final DefaultIndexImpl<Integer, String> index = new DefaultIndexImpl<>(
				INDEX_NAME, mockedStorage, transactionManager, keyConverter,
				valueConverter, comparator, null);

		below.expect(HGException.class);
		below.expectMessage("Attempting to operate on index 'sample_index' while the index is being closed.");
		index.count();
	}

	@Test
	public void returnsZero_whenThereAreNotAddedEntries() throws Exception
	{
		startupIndex();

		final long actualCount = index.count();
		assertThat(actualCount, is(0L));

		index.close();
	}

	@Test
	public void wrapsUnderlyingException_withHypergraphException()
			throws Exception
	{
		// create index and open it in usual way
		mockStorage();
		replay(mockedStorage);
		final DefaultIndexImpl<Integer, String> index = new DefaultIndexImpl<>(
				INDEX_NAME, mockedStorage, transactionManager, keyConverter,
				valueConverter, comparator, null);
		index.open();
		verify(mockedStorage);
		reset(mockedStorage);

		// after index is opened inject fake database field and call
		final Field databaseField = index.getClass().getDeclaredField(
				DATABASE_FIELD_NAME);
		databaseField.setAccessible(true);
		// close real database before use fake one
		final Database realDatabase = (Database) databaseField.get(index);
		realDatabase.close();
		// create fake database instance and imitate throwing exception
		final Database fakeDatabase = createStrictMock(Database.class);
		expect(fakeDatabase.openCursor(isNull(), anyObject(CursorConfig.class)

		)).andThrow(
				new DatabaseNotFoundException(
						"This exception is thrown by fake database."));

		replay(mockedStorage, fakeDatabase);
		// inject fake database into appropriate field
		databaseField.set(index, fakeDatabase);

		try
		{
			below.expect(HGException.class);
			below.expectMessage(allOf(
					containsString("com.sleepycat.je.DatabaseNotFoundException"),
					CoreMatchers
							.containsString("This exception is thrown by fake database.")));
			index.count();
		}
		finally
		{
			// set null value to DefaultIndexImpl.db field
			// without this setting Database.close() method on fake database
			// instance is invoked somewhere
			// and all next tests fail because Easymock expects 'close' call
			databaseField.set(index, null);
		}
	}
}
