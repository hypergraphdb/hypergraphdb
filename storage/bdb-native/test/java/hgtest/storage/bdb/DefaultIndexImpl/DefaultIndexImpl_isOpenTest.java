package hgtest.storage.bdb.DefaultIndexImpl;

import org.hypergraphdb.storage.bdb.DefaultIndexImpl;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * @author Yuriy Sechko
 */
public class DefaultIndexImpl_isOpenTest extends DefaultIndexImplTestBasis
{
	@Test
	public void indexIsOpened() throws Exception
	{
		startupIndex();

		final boolean isOpen = index.isOpen();

		assertTrue(isOpen);
		index.close();
	}

	@Test
	public void indexIsNotOpened() throws Exception
	{
		PowerMock.replayAll();
		final DefaultIndexImpl index = new DefaultIndexImpl(INDEX_NAME,
				storage, transactionManager, keyConverter, valueConverter,
				comparator);

		final boolean isOpen = index.isOpen();

		assertFalse(isOpen);
	}
}
