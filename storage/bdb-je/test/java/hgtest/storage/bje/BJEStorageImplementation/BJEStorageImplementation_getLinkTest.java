package hgtest.storage.bje.BJEStorageImplementation;

import org.hypergraphdb.HGException;
import org.testng.annotations.Test;

import static hgtest.storage.bje.TestUtils.assertExceptions;


/**
 * @author Yuriy Sechko
 */
public class BJEStorageImplementation_getLinkTest extends
		BJEStorageImplementationTestBasis
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
