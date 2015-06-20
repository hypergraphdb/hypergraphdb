package hgtest.storage.bdb.BDBStorageImplementation;

import org.hypergraphdb.HGException;
import org.testng.annotations.Test;

import static hgtest.storage.bdb.TestUtils.assertExceptions;


/**
 * @author Yuriy Sechko
 */
public class BDBStorageImplementation_getDataTest extends
		BDBStorageImplementationTestBasis
{
	@Test
	public void useNullHandle() throws Exception
	{
		final Exception expected = new HGException(
				"Failed to retrieve link with handle null");

		startup();
		try
		{
			storage.getData(null);
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
