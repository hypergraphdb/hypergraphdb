package hgtest.storage.bdb.BDBStorageImplementation;

import org.hypergraphdb.HGException;
import org.testng.annotations.Test;

import static hgtest.storage.bdb.TestUtils.assertExceptions;


/**
 * @author Yuriy Sechko
 */
public class BDBStorageImplementation_removeIncidenceSetTest extends
		BDBStorageImplementationTestBasis
{
	@Test
	public void useNullHandle() throws Exception
	{
		final Exception expected = new HGException(
				"Failed to remove incidence set of handle null: java.lang.NullPointerException");

		startup();
		try
		{
			storage.removeIncidenceSet(null);
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
