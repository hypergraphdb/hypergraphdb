package hgtest.storage.bje.BJEStorageImplementation;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.testng.annotations.Test;

import static hgtest.storage.bje.TestUtils.assertExceptions;


/**
 * @author Yuriy Sechko
 */
public class BJEStorageImplementation_removeDataTest extends
		BJEStorageImplementationTestBasis
{
	@Test
	public void useNullHandle() throws Exception
	{
		final Exception expected = new NullPointerException(
				"HGStore.remove called with a null handle.");

		startup();
		try
		{
			storage.removeData(null);
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
	public void exceptionWhileRemovingData() throws Exception
	{
		startup(new IllegalStateException("Throw exception in test case."));
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		try
		{
			storage.removeData(handle);
		}
		catch (Exception ex)
		{
			final String expectedMessage = String
					.format("Failed to remove value with handle %s: java.lang.IllegalStateException: Throw exception in test case.",
							handle);
			assertExceptions(ex, HGException.class, expectedMessage);
		}
		finally
		{
			shutdown();
		}
	}
}
