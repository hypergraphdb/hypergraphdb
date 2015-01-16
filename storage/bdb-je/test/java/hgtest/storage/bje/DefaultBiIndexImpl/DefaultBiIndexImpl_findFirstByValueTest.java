package hgtest.storage.bje.DefaultBiIndexImpl;

import org.easymock.EasyMock;
import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.bje.DefaultBiIndexImpl;
import org.hypergraphdb.transaction.HGTransactionManager;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import java.lang.reflect.Field;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

/**
 * @author Yuriy Sechko
 */
public class DefaultBiIndexImpl_findFirstByValueTest extends
		DefaultBiIndexImplTestBasis
{
	private DefaultBiIndexImpl<Integer, String> indexImpl;

	private void startupIndex()
	{
		mockStorage();
		PowerMock.replayAll();
		indexImpl = new DefaultBiIndexImpl<Integer, String>(INDEX_NAME,
				storage, transactionManager, keyConverter, valueConverter, null);
		indexImpl.open();
	}

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
			assertEquals(occurred.getClass(), expected.getClass());
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
				storage, transactionManager, keyConverter, valueConverter, null);

		try
		{
			indexImpl.findFirstByValue("some value");
		}
		catch (Exception occurred)
		{
			assertEquals(occurred.getClass(), expected.getClass());
		}
	}

    @Test
    public void transactionManagerThrowsException() throws Exception
    {
        final Exception expected = new HGException(
                "Failed to lookup index 'sample_index': java.lang.IllegalStateException");

        mockStorage();
        final HGTransactionManager fakeTransactionManager = PowerMock
                .createStrictMock(HGTransactionManager.class);
        EasyMock.expect(fakeTransactionManager.getContext()).andThrow(
                new IllegalStateException());
        PowerMock.replayAll();
        indexImpl = new DefaultBiIndexImpl<Integer, String>(INDEX_NAME,
                storage, transactionManager, keyConverter, valueConverter, null);
        indexImpl.open();
        indexImpl.addEntry(0, "red");

        // inject fake transaction manager
        final Field transactionManagerField = indexImpl.getClass()
                .getSuperclass().getDeclaredField("transactionManager");
        transactionManagerField.setAccessible(true);
        transactionManagerField.set(indexImpl, fakeTransactionManager);
        try
        {
            indexImpl.findFirstByValue("yellow");
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
}
