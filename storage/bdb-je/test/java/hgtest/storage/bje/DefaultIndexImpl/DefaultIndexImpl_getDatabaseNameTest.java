package hgtest.storage.bje.DefaultIndexImpl;


import org.hypergraphdb.storage.bje.DefaultIndexImpl;
import org.powermock.api.easymock.PowerMock;
import org.junit.Test;

import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;

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

        replay(mockedStorage);
		final DefaultIndexImpl index = new DefaultIndexImpl(null, mockedStorage,
				transactionManager, keyConverter, valueConverter, comparator, null);

		final String actual = index.getDatabaseName();

		assertEquals(actual, expected);
	}

	@Test
	public void indexNameIsNotNull() throws Exception
	{
		final String expected = "hgstore_idx_index name";

        replay(mockedStorage);
		final DefaultIndexImpl index = new DefaultIndexImpl("index name",
                mockedStorage, transactionManager, keyConverter, valueConverter,
				comparator, null);

		final String actual = index.getDatabaseName();

		assertEquals(actual, expected);
	}
}
