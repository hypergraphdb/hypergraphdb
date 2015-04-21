package hgtest.storage.bdb.BDBStorageImplementation;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.testng.annotations.Test;

import static hgtest.TestUtils.assertExceptions;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

/**
 * @author Yuriy Sechko
 */
public class BDBStorageImplementation_getDataTest extends
		BDBStorageImplementationTestBasis
{
    @Test
    public void readDataWhichIsNotStored() throws Exception
    {
        startup(1);
        final HGPersistentHandle handle = new UUIDPersistentHandle();
        final byte[] retrieved = storage.getData(handle);
        assertNull(retrieved);
        shutdown();
    }

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

    @Test
    public void readEmptyArray() throws Exception
    {
        final byte[] expected = new byte[] {};
        startup(2);
        final HGPersistentHandle handle = new UUIDPersistentHandle();
        storage.store(handle, new byte[] {});
        final byte[] retrieved = storage.getData(handle);
        assertEquals(retrieved, expected);
        shutdown();
    }

    @Test
    public void readArrayWhichContainsOneItem() throws Exception
    {
        final byte[] expected = new byte[] { 44 };
        startup(2);
        final HGPersistentHandle handle = new UUIDPersistentHandle();
        storage.store(handle, new byte[] { 44 });
        final byte[] retrieved = storage.getData(handle);
        assertEquals(retrieved, expected);
        shutdown();
    }

    @Test
    public void readArrayWhichContainsSeveralItems() throws Exception
    {
        final byte[] expected = new byte[] { 11, 22, 33 };
        startup(2);
        final HGPersistentHandle handle = new UUIDPersistentHandle();
        storage.store(handle, new byte[] { 11, 22, 33 });
        final byte[] retrieved = storage.getData(handle);
        assertEquals(retrieved, expected);
        shutdown();
    }
}
