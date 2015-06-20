package hgtest.storage.bje.BJEStorageImplementation;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.testng.annotations.Test;

import static hgtest.storage.bje.TestUtils.assertExceptions;


/**
 * @author Yuriy Sechko
 */
public class BJEStorageImplementation_removeLinkTest extends
		BJEStorageImplementationTestBasis
{

	@Test
	public void removeLinkUsingNullHandle() throws Exception
	{
		final Exception expected = new NullPointerException(
				"HGStore.remove called with a null handle.");

		startup();
		try
		{
			storage.removeLink(null);
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
	public void throwExceptionWhileRemovingLink() throws Exception
	{
		startup(new IllegalStateException("Throw exception in test case."));
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		try
		{
			storage.removeLink(handle);

		}
		catch (Exception ex)
		{
			final String expectedMessage = String
					.format("Failed to remove value with handle %s: java.lang.IllegalStateException: Throw exception in test case.",
							handle.toString());
			assertExceptions(ex, HGException.class, expectedMessage);
		}
		finally
		{
			shutdown();
		}
	}
}
