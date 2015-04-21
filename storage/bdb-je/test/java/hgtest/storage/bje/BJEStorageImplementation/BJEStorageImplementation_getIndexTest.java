package hgtest.storage.bje.BJEStorageImplementation;

import org.hypergraphdb.HGIndex;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.testng.annotations.Test;

import java.util.Comparator;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

/**
 */
public class BJEStorageImplementation_getIndexTest extends
		BJEStorageImplementationTestBasis
{
	@Test
	public void convertersAndComparatorsAreNull() throws Exception
	{
		startup(1);
		final String indexName = "sampleIndex";
		final ByteArrayConverter<Integer> keyConverter = null;
		final ByteArrayConverter<String> valueConverter = null;
		final Comparator<?> comparator = null;
		final boolean bidirectional = true;
		final boolean createIfNecessary = true;

        final HGIndex<Integer, String> createdIndex = storage.getIndex(
				indexName, keyConverter, valueConverter, comparator,
				bidirectional, createIfNecessary);

        assertNotNull(createdIndex);
		shutdown();
	}

	@Test
	public void bidirectionalFlagIsSetToFalse() throws Exception
	{
		startup(1);
		final boolean bidirectional = false;

        final HGIndex<Integer, String> createdIndex = storage.getIndex(
				"sampleIndex", null, null, null, bidirectional, true);

        assertNotNull(createdIndex);
		shutdown();
	}

	@Test
	public void createIfNecessaryFlagIsSetToFalse() throws Exception
	{
		startup();
		final boolean createIfNecessary = false;

        final HGIndex<Object, Object> createdIndex = storage.getIndex(
				"sampleIndex", null, null, null, true, createIfNecessary);

        assertNull(createdIndex);
		shutdown();
	}

	@Test
	public void indexWithTheSameNameAlreadyExists() throws Exception
	{
		startup(1);
		final String indexName = "sample index";
		final HGIndex<Object, Object> firstIndex = storage.getIndex(indexName, null, null, null, true, true);

        final HGIndex<Object, Object> secondIndex = storage.getIndex(indexName,
				null, null, null, true, true);

        assertEquals(firstIndex, secondIndex);
		shutdown();
	}

    @Test
    public void indexNameIsNull() throws Exception {
        startup(1);
        final String indexName = null;

        final HGIndex<Object, Object> createdIndex = storage.getIndex(indexName, null, null, null, true, true);

        assertNotNull(createdIndex);
        shutdown();
    }
}
