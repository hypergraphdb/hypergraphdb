package hgtest.storage.bje.DefaultIndexImpl;

import org.hypergraphdb.storage.bje.DefaultIndexImpl;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @author Yuriy Sechko
 */
public class DefaultIndexImpl_getDatabaseNameTest extends
		DefaultIndexImplTestBasis
{
	@Test
	public void indexNameIsNull() throws Exception
	{
		final String expected = "hgstore_idx_null";

		PowerMock.replayAll();
		final DefaultIndexImpl index = new DefaultIndexImpl(null, storage,
				transactionManager, keyConverter, valueConverter, comparator);

		final String actual = index.getDatabaseName();

		assertEquals(actual, expected);
	}

	@Test
	public void indexNameIsNotNull() throws Exception
	{
		final String expected = "hgstore_idx_index name";

		PowerMock.replayAll();
		final DefaultIndexImpl index = new DefaultIndexImpl("index name",
				storage, transactionManager, keyConverter, valueConverter,
				comparator);

		final String actual = index.getDatabaseName();

		assertEquals(actual, expected);
	}
}
