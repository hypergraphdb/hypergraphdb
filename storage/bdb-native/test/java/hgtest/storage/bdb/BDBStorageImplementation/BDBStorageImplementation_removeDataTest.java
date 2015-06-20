package hgtest.storage.bdb.BDBStorageImplementation;

import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.testng.annotations.Test;

import static hgtest.storage.bdb.TestUtils.assertExceptions;
import static org.testng.Assert.assertEquals;

/**
 * @author Yuriy Sechko
 */
public class BDBStorageImplementation_removeDataTest extends
		BDBStorageImplementationTestBasis
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
			assertEquals(ex.getClass(), org.hypergraphdb.HGException.class);
			final String expectedMessage = String
					.format("Failed to remove value with handle %s: java.lang.IllegalStateException: Throw exception in test case.",
							handle);
			assertEquals(ex.getMessage(), expectedMessage);
		}
		finally
		{
			shutdown();
		}
	}
}
