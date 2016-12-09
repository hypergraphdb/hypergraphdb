package hgtest.storage.bje.BJEStorageImplementation;

import static java.lang.String.format;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.junit.After;
import org.junit.Test;

import com.sleepycat.je.DatabaseNotFoundException;

public class BJEStorageImplementation_containsLinkTest extends
		BJEStorageImplementationTestBasis
{

	@Test
	public void throwsException_whenNullHandleIsSpecified() throws Exception
	{
		startup();

		below.expect(NullPointerException.class);
		storage.containsLink(null);
	}

	@Test
	public void wrapsDatabaseException_withHypergraphException()
			throws Exception
	{
		startup(new DatabaseNotFoundException("Exception in test case."));

		final HGPersistentHandle handle = new UUIDPersistentHandle();

		below.expect(HGException.class);
		below
				.expectMessage(format(
						"Failed to retrieve link with handle %s: com.sleepycat.je.DatabaseNotFoundException: (JE 5.0.34) Exception in test case.",
						handle.toString()));
		storage.containsLink(handle);
	}

	@After
	public void shutdown() throws Exception
	{
		super.shutdown();
	}
}
