package hgtest.storage.bje.DefaultBiIndexImpl;

import org.hypergraphdb.storage.bje.DefaultBiIndexImpl;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertEquals;

/**
 * Use null comparator in these test cases - it forces
 * {@link org.hypergraphdb.storage.bje.DefaultBiIndexImpl} to use default
 * Sleepycat je BTreeComparator
 * 
 * @author Yuriy Sechko
 */
public class DefaultBiIndexImpl_addEntryTest extends
		DefaultBiIndexImpl_TestBasis
{
	@Test
	public void keyIsNull() throws Exception
	{
		final Exception expected = new NullPointerException();

		mockStorage();
		PowerMock.replayAll();
		final DefaultBiIndexImpl<Integer, String> indexImpl = new DefaultBiIndexImpl(
				INDEX_NAME, storage, transactionManager, keyConverter,
				valueConverter, null);
		indexImpl.open();

		try
		{
			indexImpl.addEntry(null, "some string");
		}
		catch (Exception occurred)
		{
			assertEquals(occurred.getClass(), expected.getClass());
			assertEquals(occurred.getMessage(), expected.getMessage());
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

		mockStorage();
		PowerMock.replayAll();
		final DefaultBiIndexImpl<Integer, String> indexImpl = new DefaultBiIndexImpl(
				INDEX_NAME, storage, transactionManager, keyConverter,
				valueConverter, null);
		indexImpl.open();

		try
		{
			indexImpl.addEntry(48, null);
		}
		catch (Exception occurred)
		{
			assertEquals(occurred.getClass(), expected.getClass());
			assertEquals(occurred.getMessage(), expected.getMessage());
		}
		finally
		{
			indexImpl.close();
		}
	}

	@Test
	public void addOneEntry() throws Exception
	{
		mockStorage();
		PowerMock.replayAll();
		final DefaultBiIndexImpl<Integer, String> indexImpl = new DefaultBiIndexImpl(
				INDEX_NAME, storage, transactionManager, keyConverter,
				valueConverter, null);
		indexImpl.open();

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

		mockStorage();
		PowerMock.replayAll();
		final DefaultBiIndexImpl<Integer, String> indexImpl = new DefaultBiIndexImpl(
				INDEX_NAME, storage, transactionManager, keyConverter,
				valueConverter, null);
		indexImpl.open();

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

		mockStorage();
		PowerMock.replayAll();
		final DefaultBiIndexImpl<Integer, String> indexImpl = new DefaultBiIndexImpl(
				INDEX_NAME, storage, transactionManager, keyConverter,
				valueConverter, null);
		indexImpl.open();

		indexImpl.addEntry(4, "first value");
		indexImpl.addEntry(4, "second value");

		final String stored = indexImpl.getData(4);

		assertEquals(stored, expected);
		indexImpl.close();
	}
}
