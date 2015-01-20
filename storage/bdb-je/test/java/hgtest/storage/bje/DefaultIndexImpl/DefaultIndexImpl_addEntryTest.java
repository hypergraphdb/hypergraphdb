package hgtest.storage.bje.DefaultIndexImpl;

import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.bje.DefaultIndexImpl;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertEquals;

/**
 * @author Yuriy Sechko
 */
public class DefaultIndexImpl_addEntryTest extends DefaultIndexImplTestBasis
{
	@Test
	public void indexIsNotOpened() throws Exception
	{
		final Exception expected = new HGException(
				"Attempting to operate on index 'sample_index' while the index is being closed.");

		PowerMock.replayAll();
		index = new DefaultIndexImpl(INDEX_NAME, storage, transactionManager,
				keyConverter, valueConverter, comparator);

		try
		{
			index.addEntry(1, "one");
		}
		catch (Exception occurred)
		{
			assertEquals(occurred.getClass(), expected.getClass());
			assertEquals(occurred.getMessage(), expected.getMessage());
		}
	}

	@Test
	public void keyIsNull() throws Exception
	{
		startupIndex();

		try
		{
			index.addEntry(null, "key is null");
		}
		catch (Exception occurred)
		{
			assertEquals(occurred.getClass(), NullPointerException.class);
		}
		closeDatabase(index);
	}

	@Test
	public void valueIsNull() throws Exception
	{
		startupIndex();

		try
		{
			index.addEntry(2, null);
		}
		catch (Exception occurred)
		{
			assertEquals(occurred.getClass(), NullPointerException.class);
		}
		closeDatabase(index);
	}

	@Test
	public void addOneEntry() throws Exception
	{
		final String expected = "twenty two";

		startupIndex();

		index.addEntry(22, "twenty two");

		final String actual = index.getData(22);
		assertEquals(actual, expected);
		index.close();
	}

	@Test
	public void addSeveralDifferentEntries() throws Exception
	{
		final List<String> expected = new ArrayList<String>();
		expected.add("one");
		expected.add("two");
		expected.add("three");

		startupIndex();

		index.addEntry(1, "one");
		index.addEntry(2, "two");
		index.addEntry(3, "three");

		// read actual stored data entry by entry
		final List<String> actual = new ArrayList<String>();
		actual.add(index.getData(1));
		actual.add(index.getData(2));
		actual.add(index.getData(3));
		assertEquals(actual, expected);
		index.close();
	}

	@Test
	public void addDuplicatedKeys() throws Exception
	{
		String expected = "another one";

		startupIndex();

		index.addEntry(1, "one");
		index.addEntry(1, "another one");
		index.addEntry(2, "two");

		String actual = index.getData(1);
		assertEquals(actual, expected);
		index.close();
	}

	@Test
	public void transactionManagerThrowsException() throws Exception
	{
		final Exception expected = new HGException(
				"Failed to add entry to index 'sample_index': java.lang.IllegalStateException: This exception is thrown by fake transaction manager.");

		startupIndexWithFakeTransactionManager();

		try
		{
			index.addEntry(2, "two");
		}
		catch (Exception occurred)
		{
			assertEquals(occurred.getClass(), expected.getClass());
			assertEquals(occurred.getMessage(), expected.getMessage());
		}
		finally
		{
			index.close();
		}
	}
}
