package hgtest.storage.bje.DefaultBiIndexImpl;

import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.bje.DefaultBiIndexImpl;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import static hgtest.storage.bje.TestUtils.assertExceptions;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

/**
 * @author Yuriy Sechko
 */
public class DefaultBiIndexImpl_findFirstByValueTest extends
		DefaultBiIndexImplTestBasis
{
	@Test
	public void findByNullValue() throws Exception
	{
		final Exception expected = new NullPointerException();

		startupIndex();

		try
		{
			indexImpl.findFirstByValue(null);
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
	public void thereAreNotAddedEntries() throws Exception
	{
		startupIndex();

		final Integer actual = indexImpl
				.findFirstByValue("this value doesn't exist");

		assertNull(actual);
		indexImpl.close();
	}

	@Test
	public void thereAreSeveralEntriesButDesiredValueDoesNotExist()
			throws Exception
	{
		startupIndex();
		indexImpl.addEntry(50, "value");

		final Integer actual = indexImpl.findFirstByValue("none");

		assertNull(actual);
		indexImpl.close();
	}

	@Test
	public void thereSeveralEntries() throws Exception
	{
		final Integer expected = 2;

		startupIndex();
		indexImpl.addEntry(1, "one");
		indexImpl.addEntry(2, "two");
		indexImpl.addEntry(3, "three");

		final Integer actual = indexImpl.findFirstByValue("two");

		assertEquals(actual, expected);
		indexImpl.close();
	}

	@Test
	public void thereAreDuplicatedValues() throws Exception
	{
		final Integer expected = 1;

		startupIndex();
		indexImpl.addEntry(2, "two");
		indexImpl.addEntry(11, "one");
		indexImpl.addEntry(3, "three");
		indexImpl.addEntry(1, "one");

		final Integer actual = indexImpl.findFirstByValue("one");

		assertEquals(actual, expected);
		indexImpl.close();
	}

	@Test
	public void indexIsNotOpened() throws Exception
	{
		final Exception expected = new HGException(
				"Attempting to lookup by value index 'sample_index' while it is closed.");

		PowerMock.replayAll();
		indexImpl = new DefaultBiIndexImpl<Integer, String>(INDEX_NAME,
				storage, transactionManager, keyConverter, valueConverter,
				comparator);

		try
		{
			indexImpl.findFirstByValue("some value");
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
				"Failed to lookup index 'sample_index': java.lang.IllegalStateException: Transaction manager is fake.");

		startupIndexWithFakeTransactionManager();

		try
		{
			indexImpl.findFirstByValue("yellow");
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
