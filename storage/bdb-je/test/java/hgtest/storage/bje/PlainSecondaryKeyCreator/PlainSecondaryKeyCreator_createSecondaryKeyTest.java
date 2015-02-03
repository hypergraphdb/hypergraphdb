package hgtest.storage.bje.PlainSecondaryKeyCreator;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.SecondaryDatabase;
import hgtest.storage.bje.TestUtils;
import org.hypergraphdb.storage.bje.PlainSecondaryKeyCreator;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import java.io.File;

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
			assertEquals(occurred.getClass(), NullPointerException.class);
		}
		finally
		{
			PowerMock.verifyAll();
		}
	}

	@Test
	public void resultIsNull() throws Exception
	{
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
			assertEquals(occurred.getClass(), NullPointerException.class);
		}
		finally
		{
			PowerMock.verifyAll();
		}
	}

	@Test
	public void thereIsOneByteInEveryEntry() throws Exception
	{
		final byte[] expected = new byte[] { 22 };

		final SecondaryDatabase fakeDatabase = PowerMock
				.createStrictMock(SecondaryDatabase.class);
		PowerMock.replayAll();
		final DatabaseEntry key = new DatabaseEntry(new byte[] { 20 });
		final DatabaseEntry data = new DatabaseEntry(new byte[] { 22 });
		final DatabaseEntry result = new DatabaseEntry(new byte[1]);

		creator.createSecondaryKey(fakeDatabase, key, data, result);

		// Method equals() in DatabaseEntry class is inherited
		// from Object class.
		// So not sure about comparison of two DatabaseEntry instances.
		// Lets compare byte data instead.
		assertEquals(result.getData(), expected);
		PowerMock.verifyAll();
	}

	@Test
	public void thereAreTwoBytesInEveryEntry() throws Exception
	{
		final byte[] expected = new byte[] { 1, 2 };

		final SecondaryDatabase fakeDatabase = PowerMock
				.createStrictMock(SecondaryDatabase.class);
		PowerMock.replayAll();
		final DatabaseEntry key = new DatabaseEntry(new byte[] { 20 });
		final DatabaseEntry data = new DatabaseEntry(new byte[] { 1, 2 });
		final DatabaseEntry result = new DatabaseEntry(new byte[2]);

		creator.createSecondaryKey(fakeDatabase, key, data, result);

		assertEquals(result.getData(), expected);
		PowerMock.verifyAll();
	}

	@Test
	public void thereAreThreeBytesInEveryEntry() throws Exception
	{
		final byte[] expected = new byte[] { 10, 11, 12 };

		final SecondaryDatabase fakeDatabase = PowerMock
				.createStrictMock(SecondaryDatabase.class);
		PowerMock.replayAll();
		final DatabaseEntry key = new DatabaseEntry(new byte[] { 20 });
		final DatabaseEntry data = new DatabaseEntry(new byte[] { 10, 11, 12 });
		final DatabaseEntry result = new DatabaseEntry(new byte[3]);

		creator.createSecondaryKey(fakeDatabase, key, data, result);

		assertEquals(result.getData(), expected);
		PowerMock.verifyAll();
	}
}
