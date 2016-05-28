package hgtest.storage.bje.DefaultIndexImpl;

import static org.easymock.EasyMock.replay;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import org.hypergraphdb.storage.bje.DefaultIndexImpl;
import org.junit.Test;

public class DefaultIndexImpl_getNameTest extends DefaultIndexImplTestBasis
{
	@Test
	public void nameIsNull() throws Exception
	{
		replay(mockedStorage);

		final DefaultIndexImpl<Integer, String> index = new DefaultIndexImpl<>(
				null, mockedStorage, transactionManager, keyConverter,
				valueConverter, comparator, null);

		final String actualName = index.getName();

		assertNull(actualName);
	}

	@Test
	public void nameIsNotNull() throws Exception
	{
		replay(mockedStorage);

		final DefaultIndexImpl<Integer, String> index = new DefaultIndexImpl<>(
				"some index name", mockedStorage, transactionManager,
				keyConverter, valueConverter, comparator, null);

		final String actualName = index.getName();

		assertThat(actualName, is("some index name"));
	}
}
