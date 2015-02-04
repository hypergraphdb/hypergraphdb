package hgtest.storage.bje.DefaultBiIndexImpl;

import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.bje.DefaultBiIndexImpl;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static hgtest.storage.bje.TestUtils.assertExceptions;
import static org.testng.Assert.assertEquals;

/**
 * @author Yuriy Sechko
 */
public class DefaultBiIndexImpl_addEntryTest extends
		DefaultBiIndexImplTestBasis
{
	@Test
	public void keyIsNull() throws Exception
	{
		final Exception expected = new NullPointerException();

		startupIndex();

		try
		{
			indexImpl.addEntry(null, "some string");
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
		}
		finally
		{
			indexImpl.close();
		}
	}

	@Test
	public void valueIsNull() throws Exception
	{
		final Exception expected = new NullPointerException();

		startupIndex();

		try
		{
			indexImpl.addEntry(48, null);
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
		}
		finally
		{
			indexImpl.close();
		}
	}

	@Test
	public void addOneEntry() throws Exception
	{
		startupIndex();

		indexImpl.addEntry(1, "one");

		final String stored = indexImpl.getData(1);
		assertEquals(stored, "one");
		indexImpl.close();
	}

	@Test
	public void addSeveralDifferentEntries() throws Exception
	{
		final List<String> expected = new ArrayList<String>();
		expected.add("twenty two");
		expected.add("thirty three");
		expected.add("forty four");

		startupIndex();

		indexImpl.addEntry(22, "twenty two");
		indexImpl.addEntry(33, "thirty three");
		indexImpl.addEntry(44, "forty four");

		final List<String> stored = new ArrayList<String>();
		stored.add(indexImpl.getData(22));
		stored.add(indexImpl.getData(33));
		stored.add(indexImpl.getData(44));

		assertEquals(stored, expected);
		indexImpl.close();
	}

	@Test
	public void addDuplicatedKeys() throws Exception
	{
		final String expected = "second value";

		startupIndex();

		indexImpl.addEntry(4, "first value");
		indexImpl.addEntry(4, "second value");

		final String stored = indexImpl.getData(4);

		assertEquals(stored, expected);
		indexImpl.close();
	}

	@Test
	public void indexIsNotOpened() throws Exception
	{
		final Exception expected = new HGException(
				"Attempting to operate on index 'sample_index' while the index is being closed.");

		PowerMock.replayAll();
		final DefaultBiIndexImpl<Integer, String> indexImplSpecificForThisTestCase = new DefaultBiIndexImpl(
				INDEX_NAME, storage, transactionManager, keyConverter,
				valueConverter, comparator);
		try
		{
			indexImplSpecificForThisTestCase.addEntry(2, "two");
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
		}
	}

	@Test
	public void transactionManagerThrowsException() throws Exception
	{
		final Exception expected = new HGException(
				"Failed to add entry to index 'sample_index': java.lang.IllegalStateException: Transaction manager is fake.");

		startupIndexWithFakeTransactionManager();

		try
		{
			indexImpl.addEntry(2, "two");
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
		}
		finally
		{
			indexImpl.close();
		}
	}
}
