package hgtest.storage.bje;

import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

/**
 */
public class BJEStorageImplementation_removeIncidenceLinkTest extends
		BJEStorageImplementationTestBasis
{
	@Test
	public void handleIsNull() throws Exception
	{
		startup();
		final HGPersistentHandle handle = null;
		final HGPersistentHandle link = new UUIDPersistentHandle();
		try
		{
			storage.removeIncidenceLink(handle, link);
		}
		catch (Exception ex)
		{
			assertEquals(ex.getClass(), org.hypergraphdb.HGException.class);
			assertEquals(
					ex.getMessage(),
					"Failed to update incidence set for handle null: java.lang.NullPointerException");
		}
		finally
		{
			shutdown();
		}
	}

    @Test
    public void bothLinkAndHandleAreNull() throws Exception
    {
        startup();
        try
        {
            storage.removeIncidenceLink(null, null);
        }
        catch (Exception ex)
        {
            assertEquals(ex.getClass(), org.hypergraphdb.HGException.class);
            assertEquals(
                    ex.getMessage(),
                    "Failed to update incidence set for handle null: java.lang.NullPointerException");
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
			assertEquals(ex.getClass(), org.hypergraphdb.HGException.class);
			final String expectedMessage = String
					.format("Failed to update incidence set for handle %s: java.lang.NullPointerException",
							handle);
			assertEquals(ex.getMessage(), expectedMessage);
		}
		finally
		{
			shutdown();
		}
	}

//	@Test
//	public void thereIsOneLink() throws Exception
//	{
//		startup(2);
//		final HGPersistentHandle handle = new UUIDPersistentHandle();
//		final HGPersistentHandle link = new UUIDPersistentHandle();
//		storage.addIncidenceLink(handle, link);
//		storage.removeIncidenceLink(handle, link);
//		final HGRandomAccessResult<HGPersistentHandle> afterRemove = storage
//				.getIncidenceResultSet(handle);
//		assertFalse(afterRemove.hasNext());
//		afterRemove.close();
//		shutdown();
//	}
}
