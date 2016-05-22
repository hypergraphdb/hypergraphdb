package hgtest.storage.bje.BJEStorageImplementation;

import static java.lang.String.format;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.junit.After;
import org.junit.Test;

import com.sleepycat.je.DatabaseNotFoundException;

public class BJEStorageImplementation_containsDataTest extends
		BJEStorageImplementationTestBasis
{
	@Test
	public void throwsException_whenHandleIsNull() throws Exception
	{
		startup();

		expectedException.expect(NullPointerException.class);
		storage.containsData(null);
	}

	@Test
	public void throwsException_whenThereAreNotCorrespondingLink()
			throws Exception
	{
		startup(new DatabaseNotFoundException("Exception in test case."));
		final HGPersistentHandle handle = new UUIDPersistentHandle();

		expectedException.expect(HGException.class);
		expectedException
				.expectMessage(format(
						"Failed to retrieve link with handle %s: com.sleepycat.je.DatabaseNotFoundException: (JE 5.0.34) Exception in test case.",
						handle));
		storage.containsData(handle);
	}

	@After
	public void shutdown() throws Exception
	{
		super.shutdown();
	}
}
