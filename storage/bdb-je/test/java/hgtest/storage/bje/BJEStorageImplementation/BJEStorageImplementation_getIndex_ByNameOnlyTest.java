package hgtest.storage.bje.BJEStorageImplementation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.hypergraphdb.HGIndex;
import org.junit.After;
import org.junit.Test;

public class BJEStorageImplementation_getIndex_ByNameOnlyTest extends
		BJEStorageImplementationTestBasis
{
	@Test
	public void returnsNullIndex_whenIndexNameIsNull() throws Exception
	{
		startup();

		final HGIndex<Object, Object> retrievedIndex = storage.getIndex(null);

		assertNull(retrievedIndex);
	}

	@Test
	public void returnsNullIndex_whenThereIsNotIndexWithDesiredName()
			throws Exception
	{
		startup();

		HGIndex<Object, Object> retrievedIndex = storage
				.getIndex("there is not index with such name");

		assertNull(retrievedIndex);
	}

	@Test
	public void happyPath() throws Exception
	{
		startup(1);

		final String indexName = "sample index";
		final HGIndex<Object, Object> storedIndex = storage.getIndex(indexName,
				null, null, null, null, true, true);

		final HGIndex<Object, Object> retrievedIndex = storage
				.getIndex(indexName);

		assertEquals(storedIndex, retrievedIndex);
	}

	@After
	public void shutdown() throws Exception
	{
		super.shutdown();
	}
}
