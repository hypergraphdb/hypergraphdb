package hgtest.storage.HGStorageImplementation;

import org.hypergraphdb.HGIndex;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.testng.annotations.Test;

import java.util.Comparator;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

/**
 * @author Yuriy Sechko
 */
public class HGStorageImplementation_getIndexTest extends
		HGStorageImplementationTestBasis
{
    // Three test cases for getting index by name only
	@Test(dataProvider = "configurations_0")
	public void indexNameIsNull(final Class configuration) throws Exception
	{
		initSpecificStorageImplementation(configuration);
		final String indexName = null;

		HGIndex<Object, Object> retrievedIndex = storage.getIndex(indexName);
		assertNull(retrievedIndex);
	}

	@Test(dataProvider = "configurations_0")
	public void getNonStoredIndex(final Class configuration) throws Exception
	{
		initSpecificStorageImplementation(configuration);
		final String indexName = "sample index";

		HGIndex<Object, Object> retrievedIndex = storage.getIndex(indexName);
		assertNull(retrievedIndex);
	}

	@Test(dataProvider = "configurations_1")
	public void getIndexThatExists(final Class configuration) throws Exception
	{
		initSpecificStorageImplementation(configuration);
		final String indexName = "sample index";

		final HGIndex<Object, Object> storedIndex = storage.getIndex(indexName,
				null, null, null, true, true);
		final HGIndex<Object, Object> retrievedIndex = storage
				.getIndex(indexName);
		assertEquals(retrievedIndex, storedIndex);
	}

    // Five test cases for getting index using complex condition
    @Test(dataProvider = "configurations_1")
    public void convertersAndComparatorsAreNull(final Class configuration) throws Exception
    {
        initSpecificStorageImplementation(configuration);
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
    }

    @Test(dataProvider = "configurations_1")
    public void bidirectionalFlagIsSetToFalse(final Class configuration) throws Exception
    {
        initSpecificStorageImplementation(configuration);
        final boolean bidirectional = false;

        final HGIndex<Integer, String> createdIndex = storage.getIndex(
                "sampleIndex", null, null, null, bidirectional, true);

        assertNotNull(createdIndex);
    }

    @Test(dataProvider = "configurations_0")
    public void createIfNecessaryFlagIsSetToFalse(final Class configuration) throws Exception
    {
        initSpecificStorageImplementation(configuration);
        final boolean createIfNecessary = false;

        final HGIndex<Object, Object> createdIndex = storage.getIndex(
                "sampleIndex", null, null, null, true, createIfNecessary);

        assertNull(createdIndex);
    }

    @Test(dataProvider = "configurations_1")
    public void indexWithTheSameNameAlreadyExists(final Class configuration) throws Exception
    {
        initSpecificStorageImplementation(configuration);
        final String indexName = "sample index";
        final HGIndex<Object, Object> firstIndex = storage.getIndex(indexName, null, null, null, true, true);

        final HGIndex<Object, Object> secondIndex = storage.getIndex(indexName,
                null, null, null, true, true);

        assertEquals(firstIndex, secondIndex);
    }

    @Test(dataProvider = "configurations_1")
    public void indexNameIsNullAndHandleIsNull(final Class configuration) throws Exception {
        initSpecificStorageImplementation(configuration);
        final String indexName = null;

        final HGIndex<Object, Object> createdIndex = storage.getIndex(indexName, null, null, null, true, true);

        assertNotNull(createdIndex);
    }
}
