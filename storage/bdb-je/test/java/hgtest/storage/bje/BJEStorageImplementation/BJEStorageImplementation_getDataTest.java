package hgtest.storage.bje.BJEStorageImplementation;

import org.hypergraphdb.HGException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BJEStorageImplementation_getDataTest extends
		BJEStorageImplementationTestBasis
{
	@Test
	public void throwsException_whenNullHandleIsUsed() throws Exception
	{
		below.expect(HGException.class);
		below
				.expectMessage("Failed to retrieve link with handle null");
		storage.getData(null);
	}

	@Before
	public void startup() throws Exception
	{
		super.startup();
	}

	@After
	public void shutdown() throws Exception
	{
		super.shutdown();
	}
}
