package hgtest.storage.bje.BJEStorageImplementation;

import org.hypergraphdb.HGIndex;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class BJEStorageImplementation_getIndex_ByNameOnlyTest extends
		BJEStorageImplementationTestBasis
{
	@Test
	public void indexNameIsNull() throws Exception
	{
		startup();
		final String indexName = null;

        HGIndex<Object,Object> retrievedIndex = storage.getIndex(indexName);

        assertNull(retrievedIndex);
        shutdown();
	}

    @Test
    public void getNonStoredIndex() throws Exception {
        startup();
        final String indexName = "sample index";

        HGIndex<Object, Object> retrievedIndex = storage.getIndex(indexName);

        assertNull(retrievedIndex);
        shutdown();
    }

    @Test
    public void getIndexThatExists() throws Exception {
        startup(1);
        final String indexName = "sample index";
        final HGIndex<Object, Object> storedIndex = storage.getIndex(indexName, null, null, null, true, true);

        final HGIndex<Object, Object> retrievedIndex = storage.getIndex(indexName);

        assertEquals(retrievedIndex, storedIndex);
        shutdown();
    }
}
