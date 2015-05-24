package hgtest.storage.bdb.DefaultBiIndexImpl;

import org.hypergraphdb.storage.bdb.DefaultBiIndexImpl;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * @author Yuriy Sechko
 */
public class DefaultBiIndexImpl_isOpenTest extends DefaultBiIndexImplTestBasis
{
	@Test
	public void indexIsNotOpenedYet() throws Exception
	{
		PowerMock.replayAll();
		final DefaultBiIndexImpl indexImpl = new DefaultBiIndexImpl(INDEX_NAME,
				storage, transactionManager, keyConverter, valueConverter,
				comparator);

		assertFalse(indexImpl.isOpen());
	}

	@Test
	public void indexIsOpen() throws Exception
	{
		mockStorage();
		PowerMock.replayAll();
		final DefaultBiIndexImpl indexImpl = new DefaultBiIndexImpl(INDEX_NAME,
				storage, transactionManager, keyConverter, valueConverter,
				comparator);
		indexImpl.open();

		assertTrue(indexImpl.isOpen());
		closeDatabases(indexImpl);
	}
}
