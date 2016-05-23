package hgtest.storage.bje.BJETxLock;

import static org.junit.rules.ExpectedException.none;

import org.easymock.EasyMock;
import org.hypergraphdb.HyperGraph;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

public class BJETxLockTestBasis
{
	protected final HyperGraph mockedGraph = EasyMock
			.createStrictMock(HyperGraph.class);

	@Rule
	public final ExpectedException below = none();

	@Before
	public void resetAndReplayMock() throws Exception
	{
		EasyMock.reset(mockedGraph);
		EasyMock.replay(mockedGraph);
	}

	@After
	public void verifyMock() throws Exception
	{
		EasyMock.verify(mockedGraph);
	}
}
