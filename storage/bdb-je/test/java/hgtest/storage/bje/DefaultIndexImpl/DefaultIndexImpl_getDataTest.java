package hgtest.storage.bje.DefaultIndexImpl;

import static org.easymock.EasyMock.replay;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.bje.DefaultIndexImpl;
import org.junit.Test;

public class DefaultIndexImpl_getDataTest extends DefaultIndexImplTestBasis
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
		index.getData(2);
	}

	@Test
	public void throwsException_whenKeyIsNull() throws Exception
	{
		startupIndex();

		try
		{
			below.expect(NullPointerException.class);
			index.getData(null);
		}
		finally
		{
			index.close();
		}
	}

	@Test
	public void returnsNull_whenThereAreNotAddedEntries() throws Exception
	{
		startupIndex();

		final String data = index.getData(2);
		assertNull(data);

		index.close();
	}

	@Test
	public void returnsNull_whenThereAreSeveralEntriesAddedByDesiredEntryDoesNotExist()
			throws Exception
	{
		startupIndex();

		index.addEntry(1, "first");
		index.addEntry(2, "second");

		final String data = index.getData(3);
		assertNull(data);

		index.close();
	}

	@Test
	public void happyPath() throws Exception
	{
		startupIndex();

		index.addEntry(1, "first");
		index.addEntry(2, "second");
		index.addEntry(3, "third");

		final String actualData = index.getData(3);
		assertThat(actualData, is("third"));

		index.close();
	}

	@Test
	public void wrapsUnderlyingException_withHypergraphException()
			throws Exception
	{
		startupIndexWithFakeTransactionManager();

		try
		{
			below.expect(HGException.class);
			below.expectMessage("Failed to lookup index 'sample_index': java.lang.IllegalStateException: This exception is thrown by fake transaction manager.");
			index.getData(22);
		}
		finally
		{
			index.close();
		}
	}
}
