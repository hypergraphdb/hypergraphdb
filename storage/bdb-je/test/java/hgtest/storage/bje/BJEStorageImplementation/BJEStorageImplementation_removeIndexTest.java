package hgtest.storage.bje.BJEStorageImplementation;

import org.hypergraphdb.HGException;
import org.testng.annotations.Test;

import static hgtest.storage.bje.TestUtils.assertExceptions;


/**
 * @author Yuriy Sechko
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
			assertExceptions(occurred, HGException.class,
					"com.sleepycat.je.DatabaseNotFoundException",
					"Attempted to remove non-existent database hgstore_idx_null");
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
			assertExceptions(
					occurred,
					HGException.class,
					"com.sleepycat.je.DatabaseNotFoundException",
					"Attempted to remove non-existent database hgstore_idx_This index does not exist");
		}
		finally
		{
			shutdown();
		}
	}
}
