package hgtest.storage.bje.DefaultBiIndexImpl;

import static org.easymock.EasyMock.replay;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.bje.DefaultBiIndexImpl;
import org.junit.Test;

public class DefaultBiIndexImpl_findFirstByValueTest extends
		DefaultBiIndexImplTestBasis
{
	@Test
	public void throwsException_whenValueIsNull() throws Exception
	{
		startupIndex();

		try
		{
			below.expect(NullPointerException.class);
			indexImpl.findFirstByValue(null);
		}
		finally
		{
			indexImpl.close();
		}
	}

	@Test
	public void returnsNullValue_whenThereAreNotAddedEntries() throws Exception
	{
		startupIndex();

		final Integer actual = indexImpl
				.findFirstByValue("this value doesn't exist");
		assertNull(actual);

		indexImpl.close();
	}

	@Test
	public void returnsNullValue_whenDesiredValueDoesNotExist()
			throws Exception
	{
		startupIndex();
		indexImpl.addEntry(50, "value");

		final Integer actual = indexImpl.findFirstByValue("none");
		assertNull(actual);

		indexImpl.close();
	}

	@Test
	public void happyPath_thereSeveralUniqueValues() throws Exception
	{
		startupIndex();
		indexImpl.addEntry(1, "one");
		indexImpl.addEntry(2, "two");
		indexImpl.addEntry(3, "three");

		final Integer found = indexImpl.findFirstByValue("two");
		assertThat(found, is(2));

		indexImpl.close();
	}

	@Test
	public void returnsLastAddedValue_whenThereAreDuplicatedValues()
			throws Exception
	{
		startupIndex();
		indexImpl.addEntry(2, "two");
		indexImpl.addEntry(11, "one");
		indexImpl.addEntry(3, "three");
		indexImpl.addEntry(1, "one");

		final Integer found = indexImpl.findFirstByValue("one");

		assertThat(found, is(1));
		indexImpl.close();
	}

	@Test
	public void throwsException_whenIndexIsNotOpenedAhead() throws Exception
	{
		replay(mockedStorage);
		indexImpl = new DefaultBiIndexImpl<>(INDEX_NAME, mockedStorage,
				transactionManager, keyConverter, valueConverter, comparator,
				null);

		below.expect(HGException.class);
		below.expectMessage("Attempting to lookup by value index 'sample_index' while it is closed.");
		indexImpl.findFirstByValue("some value");
	}

	@Test
	public void wrapsUnderlyingException_withHypergraphException()
			throws Exception
	{
		startupIndexWithFakeTransactionManager();

		try
		{
			below.expect(HGException.class);
			below.expectMessage("Failed to lookup index 'sample_index': java.lang.IllegalStateException: Transaction manager is fake.");
			indexImpl.findFirstByValue("yellow");
		}
		finally
		{
			indexImpl.close();
		}
	}
}
