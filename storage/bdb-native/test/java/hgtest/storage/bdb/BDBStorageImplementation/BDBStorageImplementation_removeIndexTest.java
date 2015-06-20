package hgtest.storage.bdb.BDBStorageImplementation;

import org.hypergraphdb.HGException;
import org.testng.annotations.Test;

import static hgtest.storage.bdb.TestUtils.assertExceptions;


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
}
