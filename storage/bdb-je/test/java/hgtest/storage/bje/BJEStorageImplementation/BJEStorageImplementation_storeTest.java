package hgtest.storage.bje.BJEStorageImplementation;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.junit.After;
import org.junit.Test;

public class BJEStorageImplementation_storeTest extends
		BJEStorageImplementationTestBasis
{

	@Test
	public void throwsException_whenDataStoredUsingNullHandle()
			throws Exception
	{
		startup(1);

		expectedException.expect(NullPointerException.class);
		storage.store(null, new byte[] {});
	}

	@Test
	public void throwsException_whenLinksStoredUsingNullHandle()
			throws Exception
	{
		startup();

		expectedException.expect(NullPointerException.class);
		storage.store(null, new HGPersistentHandle[] {});
	}

	@Test
	public void throwsException_whenDataIsNull() throws Exception
	{
		startup();

		final HGPersistentHandle handle = new UUIDPersistentHandle();
		final byte[] nullData = null;

		expectedException.expect(NullPointerException.class);
		expectedException.expectMessage("Can't store null data.");
		storage.store(handle, nullData);
	}

	@Test
	public void throwsException_whenArrayOfLinksIsNull() throws Exception
	{
		startup();

		final HGPersistentHandle handle = new UUIDPersistentHandle();
		final HGPersistentHandle[] links = null;

		expectedException.expect(NullPointerException.class);
		storage.store(handle, links);
	}

	@Test
	public void throwsException_whenArrayOfLinksContainsNullHandles()
			throws Exception
	{
		startup();

		final HGPersistentHandle handle = new UUIDPersistentHandle();
		final HGPersistentHandle[] links = new HGPersistentHandle[] { null };

		expectedException.expect(NullPointerException.class);
		storage.store(handle, links);
	}

	@Test
	public void wrapsUnderlyingException_withHypergraphException()
			throws Exception
	{
		startup(new IllegalStateException("Throw exception in test case."));

		final HGPersistentHandle handle = new UUIDPersistentHandle();

		expectedException.expect(HGException.class);
		expectedException
				.expectMessage("Failed to store hypergraph link: java.lang.IllegalStateException: Throw exception in test case.");
		storage.store(handle, new HGPersistentHandle[] {});
	}

	@After
	public void shutdown() throws Exception
	{
		super.shutdown();
	}
}
