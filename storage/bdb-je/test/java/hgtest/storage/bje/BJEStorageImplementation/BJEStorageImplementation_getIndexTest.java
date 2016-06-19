package hgtest.storage.bje.BJEStorageImplementation;

import static org.junit.Assert.*;

import java.util.Comparator;

import org.hypergraphdb.HGIndex;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.junit.After;
import org.junit.Test;

public class BJEStorageImplementation_getIndexTest extends
		BJEStorageImplementationTestBasis
{
	@Test
	public void returnsValidIndex_whenConvertersAndComparatorsAreNull()
			throws Exception
	{
		startup(1);

		final String indexName = "sampleIndex";
		final ByteArrayConverter<Integer> keyConverter = null;
		final ByteArrayConverter<String> valueConverter = null;
		final Comparator<byte[]> comparator = null;
		final boolean bidirectional = true;
		final boolean createIfNecessary = true;

		final HGIndex<Integer, String> createdIndex = storage.getIndex(
				indexName, keyConverter, valueConverter, comparator, null,
				bidirectional, createIfNecessary);

		assertNotNull(createdIndex);
	}

	@Test
	public void returnsValidIndex_whenBidirectionalFlagIsSetToFalse()
			throws Exception
	{
		startup(1);

		final boolean bidirectional = false;

		final HGIndex<Integer, String> createdIndex = storage.getIndex(
				"sampleIndex", null, null, null, null, bidirectional, true);

		assertNotNull(createdIndex);
	}

	@Test
	public void returnsNull_whenCreateIfNecessaryFlagIsSetToFalse()
			throws Exception
	{
		startup();

		final boolean createIfNecessary = false;

		final HGIndex<Object, Object> createdIndex = storage.getIndex(
				"sampleIndex", null, null, null, null, true, createIfNecessary);

		assertNull(createdIndex);
	}

	@Test
	public void returnsTheSameInstance_whenIndexWithTheSameNameAlreadyExists()
			throws Exception
	{
		startup(1);

		final String indexName = "sample index";
		final HGIndex<Object, Object> firstIndex = storage.getIndex(indexName,
				null, null, null, null, true, true);

		final HGIndex<Object, Object> secondIndex = storage.getIndex(indexName,
				null, null, null, null, true, true);

		assertSame(firstIndex, secondIndex);
	}

	@Test
	public void returnsValidIndex_whenIndexNameIsNull() throws Exception
	{
		startup(1);

		final String indexName = null;

        final HGIndex<Object, Object> createdIndex = storage.getIndex(indexName, null, null, null,null,  true, true);

		assertNotNull(createdIndex);
	}

	@After
	public void shutdown() throws Exception
	{
		super.shutdown();
	}
}
