package hgtest.storage.bje.BJEStorageImplementation;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.testng.annotations.Test;

import static hgtest.storage.bje.TestUtils.assertExceptions;


/**
 * @author Yuriy Sechko
 */
public class BJEStorageImplementation_removeIncidenceLinkTest extends
		BJEStorageImplementationTestBasis
{
	@Test
	public void handleIsNull() throws Exception
	{
		final Exception expected = new HGException(
				"Failed to update incidence set for handle null: java.lang.NullPointerException");

		startup();
		final HGPersistentHandle handle = null;
		final HGPersistentHandle link = new UUIDPersistentHandle();
		try
		{
			storage.removeIncidenceLink(handle, link);
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
	public void bothLinkAndHandleAreNull() throws Exception
	{
		final Exception expected = new HGException(
				"Failed to update incidence set for handle null: java.lang.NullPointerException");

		startup();
		try
		{
			storage.removeIncidenceLink(null, null);
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
	public void linkIsNull() throws Exception
	{
		startup();
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		final HGPersistentHandle link = null;
		try
		{
			storage.removeIncidenceLink(handle, link);
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
