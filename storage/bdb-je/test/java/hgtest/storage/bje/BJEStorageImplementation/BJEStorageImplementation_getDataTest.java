package hgtest.storage.bje.BJEStorageImplementation;

import org.hypergraphdb.HGException;
import org.testng.annotations.Test;

import static hgtest.storage.bje.TestUtils.assertExceptions;

/**
 * @author Yuriy Sechko
 */
public class BJEStorageImplementation_getDataTest extends
		BJEStorageImplementationTestBasis
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
