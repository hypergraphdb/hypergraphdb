package hgtest.storage.bje.PlainSecondaryKeyCreator;


import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.SecondaryDatabase;
import org.hypergraphdb.storage.bje.PlainSecondaryKeyCreator;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.powermock.api.easymock.PowerMock;
import org.junit.Test;

import static hgtest.storage.bje.TestUtils.assertExceptions;
import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.rules.ExpectedException.none;

public class PlainSecondaryKeyCreator_createSecondaryKeyTest
{
	protected final PlainSecondaryKeyCreator creator = PlainSecondaryKeyCreator
			.getInstance();

    @Rule
    public final ExpectedException below = none();

	@Test
	public void doesNotFails_whenSecondaryDatabaseIsNull() throws Exception
	{
		final DatabaseEntry key = new DatabaseEntry(new byte[] { 20 });
		final DatabaseEntry data = new DatabaseEntry(new byte[] { 22 });
		final DatabaseEntry result = new DatabaseEntry(new byte[1]);

		creator.createSecondaryKey(null, key, data, result);
	}

	@Test
	public void doesNotFail_whenKeyIsNull() throws Exception
	{
		final SecondaryDatabase fakeDatabase = createStrictMock(SecondaryDatabase.class);
		replay(fakeDatabase);
		final DatabaseEntry data = new DatabaseEntry(new byte[] { 22 });
		final DatabaseEntry result = new DatabaseEntry(new byte[1]);

		creator.createSecondaryKey(fakeDatabase, null, data, result);

		verify(fakeDatabase);
	}

	@Test
	public void throwsException_whenDataIsNull() throws Exception
	{
		final SecondaryDatabase fakeDatabase = createStrictMock(SecondaryDatabase.class);
		replay(fakeDatabase);
		final DatabaseEntry key = new DatabaseEntry(new byte[] { 20 });
		final DatabaseEntry result = new DatabaseEntry(new byte[1]);

		try
		{
            below.expect(NullPointerException.class);
			creator.createSecondaryKey(fakeDatabase, key, null, result);
		}
		finally
		{
			verify(fakeDatabase);
		}
	}

	@Test
	public void throwsException_whenResultIsNull() throws Exception
	{
		final SecondaryDatabase fakeDatabase = createStrictMock(SecondaryDatabase.class);
        replay(fakeDatabase);
		final DatabaseEntry key = new DatabaseEntry(new byte[] { 20 });
		final DatabaseEntry data = new DatabaseEntry(new byte[] { 22 });

		try
		{
            below.expect(NullPointerException.class);
			creator.createSecondaryKey(fakeDatabase, key, data, null);
		}
		finally
		{
            verify(fakeDatabase);
		}
	}

	@Test
	public void happyPath_whenThereIsOneByteInEachEntry() throws Exception
	{
		final DatabaseEntry expected = new DatabaseEntry(new byte[] { 22 });

		final SecondaryDatabase fakeDatabase = createStrictMock(SecondaryDatabase.class);
        replay(fakeDatabase);
		final DatabaseEntry key = new DatabaseEntry(new byte[] { 20 });
		final DatabaseEntry data = new DatabaseEntry(new byte[] { 22 });
		final DatabaseEntry result = new DatabaseEntry(new byte[1]);

		creator.createSecondaryKey(fakeDatabase, key, data, result);

		assertEquals(result, expected);
        verify(fakeDatabase);
	}

	@Test
	public void happyPath_whenThereAreTwoBytesInEachEntry() throws Exception
	{
		final DatabaseEntry expected = new DatabaseEntry(new byte[] { 1, 2 });

		final SecondaryDatabase fakeDatabase = createStrictMock(SecondaryDatabase.class);
        replay(fakeDatabase);
		final DatabaseEntry key = new DatabaseEntry(new byte[] { 20 });
		final DatabaseEntry data = new DatabaseEntry(new byte[] { 1, 2 });
		final DatabaseEntry result = new DatabaseEntry(new byte[2]);

		creator.createSecondaryKey(fakeDatabase, key, data, result);

		assertEquals(result, expected);
        verify(fakeDatabase);
	}

	@Test
	public void happyPath_whenThereAreThreeBytesInEachEntry() throws Exception
	{
		final DatabaseEntry expected = new DatabaseEntry(new byte[] { 10, 11,
				12 });

		final SecondaryDatabase fakeDatabase = createStrictMock(SecondaryDatabase.class);
        replay(fakeDatabase);
		final DatabaseEntry key = new DatabaseEntry(new byte[] { 20 });
		final DatabaseEntry data = new DatabaseEntry(new byte[] { 10, 11, 12 });
		final DatabaseEntry result = new DatabaseEntry(new byte[3]);

		creator.createSecondaryKey(fakeDatabase, key, data, result);

		assertEquals(result, expected);
        verify(fakeDatabase);
	}
}
