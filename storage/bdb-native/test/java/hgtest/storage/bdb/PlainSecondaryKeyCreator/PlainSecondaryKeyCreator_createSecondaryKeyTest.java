package hgtest.storage.bdb.PlainSecondaryKeyCreator;

import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.SecondaryDatabase;
import org.hypergraphdb.storage.bdb.PlainSecondaryKeyCreator;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import static hgtest.storage.bdb.TestUtils.assertExceptions;
import static org.testng.Assert.assertEquals;

/**
 * @author Yuriy Sechko
 */
public class PlainSecondaryKeyCreator_createSecondaryKeyTest
{
	protected final PlainSecondaryKeyCreator creator = PlainSecondaryKeyCreator
			.getInstance();

	@Test
	public void secondaryDatabaseIsNull() throws Exception
	{
		final DatabaseEntry key = new DatabaseEntry(new byte[] { 20 });
		final DatabaseEntry data = new DatabaseEntry(new byte[] { 22 });
		final DatabaseEntry result = new DatabaseEntry(new byte[1]);

		creator.createSecondaryKey(null, key, data, result);
	}

	@Test
	public void keyIsNull() throws Exception
	{
		final SecondaryDatabase fakeDatabase = PowerMock
				.createStrictMock(SecondaryDatabase.class);
		PowerMock.replayAll();
		final DatabaseEntry data = new DatabaseEntry(new byte[] { 22 });
		final DatabaseEntry result = new DatabaseEntry(new byte[1]);

		creator.createSecondaryKey(fakeDatabase, null, data, result);

		PowerMock.verifyAll();
	}

	@Test
	public void dataIsNull() throws Exception
	{
		final Exception expected = new NullPointerException();

		final SecondaryDatabase fakeDatabase = PowerMock
				.createStrictMock(SecondaryDatabase.class);
		PowerMock.replayAll();
		final DatabaseEntry key = new DatabaseEntry(new byte[] { 20 });
		final DatabaseEntry result = new DatabaseEntry(new byte[1]);

		try
		{
			creator.createSecondaryKey(fakeDatabase, key, null, result);
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
		}
		finally
		{
			PowerMock.verifyAll();
		}
	}

	@Test
	public void resultIsNull() throws Exception
	{
		final Exception expected = new NullPointerException();

		final SecondaryDatabase fakeDatabase = PowerMock
				.createStrictMock(SecondaryDatabase.class);
		PowerMock.replayAll();
		final DatabaseEntry key = new DatabaseEntry(new byte[] { 20 });
		final DatabaseEntry data = new DatabaseEntry(new byte[] { 22 });

		try
		{
			creator.createSecondaryKey(fakeDatabase, key, data, null);
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
		}
		finally
		{
			PowerMock.verifyAll();
		}
	}

	@Test
	public void thereIsOneByteInEachEntry() throws Exception
	{
		final DatabaseEntry expected = new DatabaseEntry(new byte[] { 22 });

		final SecondaryDatabase fakeDatabase = PowerMock
				.createStrictMock(SecondaryDatabase.class);
		PowerMock.replayAll();
		final DatabaseEntry key = new DatabaseEntry(new byte[] { 20 });
		final DatabaseEntry data = new DatabaseEntry(new byte[] { 22 });
		final DatabaseEntry result = new DatabaseEntry(new byte[1]);

		creator.createSecondaryKey(fakeDatabase, key, data, result);

		assertEquals(result, expected);
		PowerMock.verifyAll();
	}

	@Test
	public void thereAreTwoBytesInEachEntry() throws Exception
	{
		final DatabaseEntry expected = new DatabaseEntry(new byte[] { 1, 2 });

		final SecondaryDatabase fakeDatabase = PowerMock
				.createStrictMock(SecondaryDatabase.class);
		PowerMock.replayAll();
		final DatabaseEntry key = new DatabaseEntry(new byte[] { 20 });
		final DatabaseEntry data = new DatabaseEntry(new byte[] { 1, 2 });
		final DatabaseEntry result = new DatabaseEntry(new byte[2]);

		creator.createSecondaryKey(fakeDatabase, key, data, result);

		assertEquals(result, expected);
		PowerMock.verifyAll();
	}

	@Test
	public void thereAreThreeBytesInEachEntry() throws Exception
	{
		final DatabaseEntry expected = new DatabaseEntry(new byte[] { 10, 11,
				12 });

		final SecondaryDatabase fakeDatabase = PowerMock
				.createStrictMock(SecondaryDatabase.class);
		PowerMock.replayAll();
		final DatabaseEntry key = new DatabaseEntry(new byte[] { 20 });
		final DatabaseEntry data = new DatabaseEntry(new byte[] { 10, 11, 12 });
		final DatabaseEntry result = new DatabaseEntry(new byte[3]);

		creator.createSecondaryKey(fakeDatabase, key, data, result);

		assertEquals(result, expected);
		PowerMock.verifyAll();
	}
}
