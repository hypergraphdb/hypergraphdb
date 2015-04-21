package hgtest.storage.bdb.BDBStorageImplementation;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGIndex;
import org.testng.annotations.Test;

import static hgtest.TestUtils.assertExceptions;
import static org.testng.Assert.assertNull;

/**
 * @author Yuriy Sechko
 */
public class BDBStorageImplementation_removeIndexTest extends
		BDBStorageImplementationTestBasis
{
	@Test
	public void indexNameIsNull() throws Exception
	{
		final Exception expected = new HGException(
				"java.io.FileNotFoundException: No such file or directory");

		startup();
		final String indexName = null;
		try
		{
			storage.removeIndex(indexName);
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
		}
		finally
		{
			shutdown();
		}
	}

	@Test
	public void removeIndexWhichIsNotStored() throws Exception
	{
		final Exception expected = new HGException(
				"java.io.FileNotFoundException: No such file or directory");

		startup();
		try
		{
			storage.removeIndex("This index does not exist");
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
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
		storage.getIndex(indexName, null, null, null, true, true);
		storage.removeIndex(indexName);
		final HGIndex<Object, Object> removedIndex = storage.getIndex(
				indexName, null, null, null, true, false);
		assertNull(removedIndex);
		shutdown();
	}
}
