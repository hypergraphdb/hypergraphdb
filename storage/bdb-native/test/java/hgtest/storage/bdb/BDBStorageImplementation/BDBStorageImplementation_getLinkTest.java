package hgtest.storage.bdb.BDBStorageImplementation;

import org.hypergraphdb.HGException;
import org.testng.annotations.Test;

import static hgtest.storage.bdb.TestUtils.assertExceptions;


/**
 * @author Yuriy Sechko
 */
public class BDBStorageImplementation_getLinkTest extends
		BDBStorageImplementationTestBasis
{
	@Test
	public void getLinksUsingNullHandle() throws Exception
	{
		final Exception expected = new HGException(
				"Failed to retrieve link with handle null");

		startup();
		try
		{
			storage.getLink(null);
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
