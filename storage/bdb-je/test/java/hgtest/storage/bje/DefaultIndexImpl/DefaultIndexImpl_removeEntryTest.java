package hgtest.storage.bje.DefaultIndexImpl;

import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.bje.DefaultIndexImpl;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static hgtest.storage.bje.TestUtils.assertExceptions;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

/**
 * @author Yuriy Sechko
 */
public class DefaultIndexImpl_removeEntryTest extends DefaultIndexImplTestBasis
{
	@Test
	public void indexIsNotOpened() throws Exception
	{
		final Exception expected = new HGException(
				"Attempting to operate on index 'sample_index' while the index is being closed.");

		PowerMock.replayAll();
		final DefaultIndexImpl<Integer, String> index = new DefaultIndexImpl<Integer, String>(
				INDEX_NAME, storage, transactionManager, keyConverter,
				valueConverter, comparator);

		try
		{
			index.removeEntry(1, "some value");
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
		}
	}

	@Test
	public void thereAreNotAddedEntries() throws Exception
	{
		startupIndex();

		index.removeEntry(22, "twenty two");

		index.close();
	}

	@Test
	public void keyIsNull() throws Exception
	{

		startupIndex();
		try
		{
			index.removeEntry(null, "some value");
		}
		catch (Exception occurred)
		{
			assertEquals(occurred.getClass(), NullPointerException.class);
		}
		finally
		{
			closeDatabase(index);

		}
	}

	@Test
	public void valueIsNull() throws Exception
	{
		startupIndex();
		try
		{
			index.removeEntry(22, null);
		}
		catch (Exception occurred)
		{
			assertEquals(occurred.getClass(), NullPointerException.class);
		}
		finally
		{
			closeDatabase(index);
		}
	}

	@Test
	public void thereIsOneDesiredEntry() throws Exception
	{
		startupIndex();
		index.addEntry(2, "two");

		index.removeEntry(2, "two");

		final String actual = index.getData(2);
		assertNull(actual);
		index.close();
	}

	@Test
	public void thereAreSeveralEntriesButDesiredEntryDoesNotExist()
			throws Exception
	{
		final List<String> expected = new ArrayList<String>();
		expected.add("one");
		expected.add("two");
		expected.add("three");

		startupIndex();
		index.addEntry(1, "one");
		index.addEntry(2, "two");
		index.addEntry(3, "three");

		index.removeEntry(5, "five");

		final List<String> actual = new ArrayList<String>();
		actual.add(index.getData(1));
		actual.add(index.getData(2));
		actual.add(index.getData(3));
		assertEquals(actual, expected);
		index.close();

	}

	@Test
	public void thereAreSeveralEntriesAddedAndDesiredEntryExists()
			throws Exception
	{
		final List<String> expected = new ArrayList<String>();
		expected.add("one");
		expected.add("two");
		expected.add(null);

		startupIndex();
		index.addEntry(1, "one");
		index.addEntry(2, "two");
		index.addEntry(3, "three");

		index.removeEntry(3, "three");

		final List<String> actual = new ArrayList<String>();
		actual.add(index.getData(1));
		actual.add(index.getData(2));
		actual.add(index.getData(3));
		assertEquals(actual, expected);
		index.close();
	}

	@Test
	public void thereAreDuplicatedEntries() throws Exception
	{
		final List<String> expected = new ArrayList<String>();
		expected.add(null);
		expected.add("two");

		startupIndex();
		index.addEntry(1, "one");
		index.addEntry(1, "one");
		index.addEntry(2, "two");

		index.removeEntry(1, "one");

		final List<String> actual = new ArrayList<String>();
		actual.add(index.getData(1));
		actual.add(index.getData(2));
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
			index.removeEntry(1, "one");
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
