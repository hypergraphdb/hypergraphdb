package hgtest.storage.bje.BJEStorageImplementation;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.testng.annotations.Test;

import static hgtest.storage.bje.TestUtils.assertExceptions;

/**
 * @author Yuriy Sechko
 */
public class BJEStorageImplementation_addIncidenceLinkTest extends
		BJEStorageImplementationTestBasis
{
	@Test
	public void linkIsNull() throws Exception
	{
		startup();
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		final HGPersistentHandle link = null;

		try
		{
			storage.addIncidenceLink(handle, link);
		}
		catch (Exception ex)
		{
			final String expectedMessage = String
					.format("Failed to update incidence set for handle %s: java.lang.NullPointerException",
							handle);
			assertExceptions(ex, HGException.class, expectedMessage);
		}
		finally
		{
			shutdown();
		}
	}
}
