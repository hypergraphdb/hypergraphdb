package hgtest.storage.bje.BJEStorageImplementation;

import static java.lang.String.format;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.junit.After;
import org.junit.Test;

public class BJEStorageImplementation_removeLinkTest extends
		BJEStorageImplementationTestBasis
{

	@Test
	public void throwsException_whenHandleIsNull() throws Exception
	{
		startup();

		expectedException.expect(NullPointerException.class);
		expectedException
				.expectMessage("HGStore.remove called with a null handle.");
		storage.removeLink(null);
	}

	@Test
	public void wrapsUnderlyingException_whenHypergraphException()
			throws Exception
	{
		startup(new IllegalStateException("Throw exception in test case."));

		final HGPersistentHandle handle = new UUIDPersistentHandle();

		expectedException.expect(HGException.class);
		expectedException
				.expectMessage(format(
						"Failed to remove value with handle %s: java.lang.IllegalStateException: Throw exception in test case.",
						handle.toString()));
		storage.removeLink(handle);

	}

	@After
	public void shutdown() throws Exception
	{
		super.shutdown();
	}
}
