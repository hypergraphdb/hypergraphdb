package hgtest.storage.bje.BJEStorageImplementation;

import org.hypergraphdb.HGIndex;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

/**
 */
public class BJEStorageImplementation_removeIndexTest extends
		BJEStorageImplementationTestBasis
{
	@Test
	public void indexNameIsNull() throws Exception
	{
		startup();
		final String indexName = null;
		try
		{
			storage.removeIndex(indexName);
		}
		catch (Exception occurred)
		{
			assertEquals(occurred.getClass(),
					org.hypergraphdb.HGException.class);
			assertEquals(
					occurred.getMessage(),
					"com.sleepycat.je.DatabaseNotFoundException: (JE 5.0.34) Attempted to remove non-existent database hgstore_idx_null");
		}
		finally
		{
			shutdown();
		}
	}

	@Test
	public void removeIndexWhichIsNotStored() throws Exception
	{
		startup();
		try
		{
			storage.removeIndex("This index does not exist");
		}
		catch (Exception occurred)
		{
			assertEquals(occurred.getClass(),
					org.hypergraphdb.HGException.class);
			assertEquals(
					occurred.getMessage(),
					"com.sleepycat.je.DatabaseNotFoundException: (JE 5.0.34) Attempted to remove non-existent database hgstore_idx_This index does not exist");
		}
		finally
		{
			shutdown();
		}
	}

	@Test
	public void removeIndexWhichExists() throws Exception
	{
		startup(1);
		final String indexName = "sample index";
		storage.getIndex(indexName,
				null, null, null, true, true);
		storage.removeIndex(indexName);
        final HGIndex<Object, Object> removedIndex = storage.getIndex(indexName, null, null, null, true, false);
        assertNull(removedIndex);
        shutdown();
	}
}
