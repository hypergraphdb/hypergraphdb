package hgtest.storage.bje.DefaultBiIndexImpl;

import static hgtest.storage.bje.TestUtils.assertExceptions;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.easymock.EasyMock;
import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.bje.DefaultBiIndexImpl;
import org.hypergraphdb.transaction.HGTransactionManager;
import org.junit.Ignore;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

public class DefaultBiIndexImpl_countKeysTest extends
		DefaultBiIndexImplTestBasis
{
	@Test
	public void useNullValue() throws Exception
	{
		final Exception expected = new NullPointerException();

		startupIndex();

		try
		{
			indexImpl.countKeys(null);
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

	@Ignore("Implementation has been changed, need to clarify the intention of test case")
	@Test
	public void thereAreNotEntriesAdded() throws Exception
	{
		final long expected = 0;

		startupIndex();

		final long actual = indexImpl.countKeys("this value doesn't exist");

		assertEquals(actual, expected);
		indexImpl.close();
	}

	@Ignore("Implementation has been changed, need to clarify the intention of test case")
	@Test
	public void thereAreSeveralEntriesByDesiredValueDoesNotExist()
			throws Exception
	{
		final long expected = 0;

		startupIndex();
		indexImpl.addEntry(1, "one");
		indexImpl.addEntry(2, "two");
		indexImpl.addEntry(3, "three");

		final long actual = indexImpl.countKeys("none");

		assertEquals(actual, expected);
		indexImpl.close();
	}

	@Test
	@Ignore("Implementation has been changed, need to clarify the intention of test case")
	public void thereIsOnDesiredValue() throws Exception
	{
		final long expected = 1;

		startupIndex();
		indexImpl.addEntry(22, "twenty two");
		indexImpl.addEntry(33, "thirty three");

		final long actual = indexImpl.countKeys("twenty two");

		assertEquals(actual, expected);
		indexImpl.close();
	}

	@Test
	@Ignore("Implementation has been changed, need to clarify the intention of test case")
	public void thereAreSeveralDesiredValues() throws Exception
	{
		final long expected = 2;

		startupIndex();
		indexImpl.addEntry(1, "one");
		indexImpl.addEntry(2, "two");
		indexImpl.addEntry(11, "one");

		final long actual = indexImpl.countKeys("one");

		assertEquals(actual, expected);
		indexImpl.close();
	}

	@Test
	public void indexIsNotOpened() throws Exception
	{
		final Exception expected = new HGException("");

        replay(mockedStorage);
		indexImpl = new DefaultBiIndexImpl<Integer, String>(INDEX_NAME,
				mockedStorage, transactionManager, keyConverter,
				valueConverter, comparator, null);

		try
		{
			indexImpl.countKeys("some value");
		}
		catch (Exception occurred)
		{
			assertEquals(occurred.getClass(), expected.getClass());
		}
	}

	@Test
	public void transactionManagerThrowsException() throws Exception
	{
		final Exception expected = new NullPointerException(
				"This exception is thrown by fake transaction manager.");

		mockStorage();
		final HGTransactionManager fakeTransactionManager = PowerMock
				.createStrictMock(HGTransactionManager.class);
		EasyMock.replay(mockedStorage, fakeTransactionManager);
		final DefaultBiIndexImpl<Integer, String> indexImpl = new DefaultBiIndexImpl<Integer, String>(
				INDEX_NAME, mockedStorage, fakeTransactionManager,
				keyConverter, valueConverter, comparator, null);
		indexImpl.open();

		try
		{
			indexImpl.countKeys("yellow");
		}
		catch (Exception occurred)
		{
			assertEquals(occurred.getClass(), expected.getClass());
			assertNull(occurred.getMessage());
		}
		finally
		{
			indexImpl.close();
		}
	}
}
