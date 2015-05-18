package hgtest.storage.bdb.BDBStorageImplementation;

import com.sleepycat.db.DatabaseException;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertNull;

/**
 * @author Yuriy Sechko
 */
public class BDBStorageImplementation_containsLinkTest extends
		BDBStorageImplementationTestBasis
{

	@Test
	public void checkExistenceOfLinkUsingNullHandle() throws Exception
	{
		startup();
		try
		{
			storage.containsLink(null);
		}
		catch (Exception ex)
		{
			assertEquals(ex.getClass(), NullPointerException.class);
			assertNull(ex.getMessage());
		}
		finally
		{
			shutdown();
		}
	}

    // TODO investigate whether it is possible to imitate checked DatabaseException
	@Test(enabled = false)
	public void exceptionWhileCheckingExistenceOfLink() throws Exception
	{
		startup(new DatabaseException("Exception in test case."));
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		try
		{
			storage.containsLink(handle);
		}
		catch (Exception ex)
		{
			assertEquals(ex.getClass(), org.hypergraphdb.HGException.class);
			final String expectedMessage = String
					.format("Failed to retrieve link with handle %s: com.sleepycat.je.DatabaseNotFoundException: (JE 5.0.34) Exception in test case.",
							handle.toString());
			assertEquals(ex.getMessage(), expectedMessage);
		}
		finally
		{
			shutdown();
		}
	}
}
