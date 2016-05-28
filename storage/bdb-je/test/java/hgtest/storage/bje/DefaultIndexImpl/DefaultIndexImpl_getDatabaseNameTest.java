package hgtest.storage.bje.DefaultIndexImpl;

import static org.easymock.EasyMock.replay;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import org.hypergraphdb.storage.bje.DefaultIndexImpl;
import org.junit.Before;
import org.junit.Test;

public class DefaultIndexImpl_getDatabaseNameTest extends
		DefaultIndexImplTestBasis
{
	@Test
	public void nullIsAppendedToTheDefaultName_whenIndexNameIsNull()
			throws Exception
	{
		final DefaultIndexImpl<Integer, String> index = new DefaultIndexImpl<>(
				null, mockedStorage, transactionManager, keyConverter,
				valueConverter, comparator, null);

		final String actualName = index.getDatabaseName();

		assertThat(actualName, is("hgstore_idx_null"));
	}

	@Test
	public void exactIndexNameIsAppended_whenIndexNameIsNotNull()
			throws Exception
	{
		final DefaultIndexImpl<Integer, String> index = new DefaultIndexImpl<>(
				"index name", mockedStorage, transactionManager, keyConverter,
				valueConverter, comparator, null);

		final String actualName = index.getDatabaseName();

		assertThat(actualName, is("hgstore_idx_index name"));
	}

	@Before
	public void startup()
	{
		replay(mockedStorage);
	}
}
