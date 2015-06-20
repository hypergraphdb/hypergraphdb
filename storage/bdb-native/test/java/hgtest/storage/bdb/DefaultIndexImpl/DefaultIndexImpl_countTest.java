package hgtest.storage.bdb.DefaultIndexImpl;

import com.sleepycat.db.Database;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.StatsConfig;
import com.sleepycat.db.Transaction;
import org.easymock.EasyMock;
import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.bdb.DefaultIndexImpl;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import java.lang.reflect.Field;

import static hgtest.storage.bdb.TestUtils.assertExceptions;
import static org.testng.Assert.assertEquals;

/**
 * @author Yuriy Sechko
 */
public class DefaultIndexImpl_countTest extends DefaultIndexImplTestBasis
{
	@Test
	public void indexIsNotOpened() throws Exception
	{
		final Exception expected = new NullPointerException();

		PowerMock.replayAll();
		final DefaultIndexImpl<Integer, String> index = new DefaultIndexImpl<Integer, String>(
				INDEX_NAME, storage, transactionManager, keyConverter,
				valueConverter, comparator);

		try
		{
			index.count();
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
		}

	}

	@Test
	public void thereAreNotAddedEntries() throws Exception
	{
		final long expected = 0;

		startupIndex();

		final long actual = index.count();

		assertEquals(actual, expected);
		index.close();
	}

	@Test
	public void thereIsOneAddedEntry() throws Exception
	{
		final long expected = 1;

		startupIndex();
		index.addEntry(0, "0");

		final long actual = index.count();

		assertEquals(actual, expected);
		index.close();
	}

	@Test
	public void thereAreTwoEntriesAdded() throws Exception
	{
		final long expected = 2;

		startupIndex();
		index.addEntry(0, "0");
		index.addEntry(1, "1");

		final long actual = index.count();

		assertEquals(actual, expected);
		index.close();
	}

	@Test
	public void thereAreSeveralEntriesAdded() throws Exception
	{
		final long expected = 3;

		startupIndex();
		index.addEntry(0, "0");
		index.addEntry(1, "1");
		index.addEntry(2, "2");

		final long actual = index.count();

		assertEquals(actual, expected);
		index.close();
	}

	@Test
	public void databaseThrowsException() throws Exception
	{
		System.out.println("databaseThrowsException test");
		final Exception expected = new HGException(
				"com.sleepycat.db.DatabaseException: This exception is thrown by fake database.");

		mockStorage();
		PowerMock.replayAll();
		final DefaultIndexImpl<Integer, String> index = new DefaultIndexImpl<Integer, String>(
				INDEX_NAME, storage, transactionManager, keyConverter,
				valueConverter, comparator);
		index.open();
		PowerMock.verifyAll();
		PowerMock.resetAll();

		final Field databaseField = index.getClass().getDeclaredField(
				DATABASE_FIELD_NAME);
		databaseField.setAccessible(true);
		final Database realDatabase = (Database) databaseField.get(index);
		realDatabase.close();
		final Database fakeDatabase = PowerMock
				.createStrictMock(Database.class);
		EasyMock.expect(
				fakeDatabase.getStats(EasyMock.<Transaction> anyObject(),
						EasyMock.<StatsConfig> anyObject())).andThrow(
				new DatabaseException(
						"This exception is thrown by fake database."));
		PowerMock.replayAll();
		databaseField.set(index, fakeDatabase);

		try
		{
			index.count();
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
		}
		databaseField.set(index, null);
	}
}
