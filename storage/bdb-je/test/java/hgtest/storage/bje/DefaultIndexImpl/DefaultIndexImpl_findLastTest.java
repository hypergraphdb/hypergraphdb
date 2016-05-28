package hgtest.storage.bje.DefaultIndexImpl;

import static hgtest.storage.bje.TestUtils.assertExceptions;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.bje.DefaultIndexImpl;
import org.junit.Test;

public class DefaultIndexImpl_findLastTest extends DefaultIndexImplTestBasis
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
		index.findLast(0);
	}

	@Test
	public void throwsException_whenKeyIsNull() throws Exception
	{
		startupIndex();

		try
		{
			below.expect(NullPointerException.class);
			index.findLast(null);
		}
		finally
		{
			index.close();
		}
	}

	@Test
	public void thereAreNotAddedEntries() throws Exception
	{
		startupIndex();

		final String found = index.findLast(28);

		assertNull(found);
		index.close();
	}

	@Test
	public void thereIsOneEntry() throws Exception
	{
		final String expected = "first";

		startupIndex();

		index.addEntry(1, "first");

		final String actual = index.findLast(1);

		assertEquals(actual, expected);
		index.close();
	}

	@Test
	public void thereAreSeveralEntriesAddedButDesiredEntryDoesNotExist()
			throws Exception
	{
		startupIndex();

		index.addEntry(1, "first");
		index.addEntry(2, "second");

		final String actual = index.findLast(50);
		// TODO what the reason, findLast() returns 'second' but not null?
		assertEquals(actual, "second");
		index.close();
	}

	@Test
	public void thereAreSeveralEntriesAddedAndDesiredEntryExists()
			throws Exception
	{
		final String expected = "fifty";

		startupIndex();

		index.addEntry(1, "first");
		index.addEntry(2, "second");
		index.addEntry(50, "fifty");

		final String actual = index.findLast(50);

		assertEquals(actual, expected);
		index.close();
	}

	@Test
	public void thereAreEntriesWithTheSameKey() throws Exception
	{
		final String expected = "two";

		startupIndex();

		index.addEntry(1, "one");
		index.addEntry(2, "two");
		index.addEntry(2, "second");

		final String actual = index.findLast(2);

		assertEquals(actual, expected);
		index.close();
	}

	@Test
	public void transactionManagerThrowsException() throws Exception
	{
		final Exception expected = new HGException(
				"Failed to lookup index 'sample_index': java.lang.IllegalStateException: This exception is thrown by fake transaction manager.");

		startupIndexWithFakeTransactionManager();

		try
		{
			index.findLast(2);
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
		}
		finally
		{
			index.close();
		}
	}
}
