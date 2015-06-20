package hgtest.storage.bje.DefaultIndexImpl;

import org.easymock.EasyMock;
import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.bje.*;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import static hgtest.storage.bje.TestUtils.assertExceptions;


/**
 * @author Yuriy Sechko
 */
public class DefaultIndexImpl_openTest extends DefaultIndexImplTestBasis
{
	@Test
	public void indexNameIsNull() throws Exception
	{
		mockStorage();
		PowerMock.replayAll();
		final DefaultIndexImpl indexImpl = new DefaultIndexImpl(null, storage,
				transactionManager, keyConverter, valueConverter, comparator);

		// no exception here
		// just because (DB_NAME_PREFIX + name) becomes "hgstore_idx_null";
		indexImpl.open();

		closeDatabase(indexImpl);
	}

	@Test
	public void transactionManagerIsNull() throws Exception
	{
		startupEnvironment();
		PowerMock.replayAll();
		final DefaultIndexImpl indexImpl = new DefaultIndexImpl(INDEX_NAME,
				storage, null, keyConverter, valueConverter, comparator);

		// no exception here
		closeDatabase(indexImpl);

		closeDatabase(indexImpl);
	}

	@Test
	public void keyConverterIsNull() throws Exception
	{
		startupEnvironment();
		mockStorage();
		PowerMock.replayAll();
		final DefaultIndexImpl indexImpl = new DefaultIndexImpl(INDEX_NAME,
				storage, transactionManager, null, valueConverter, comparator);

		// no exception here
		indexImpl.open();

		closeDatabase(indexImpl);
	}

	@Test
	public void valueConverterIsNull() throws Exception
	{
		startupEnvironment();
		mockStorage();
		PowerMock.replayAll();
		final DefaultIndexImpl indexImpl = new DefaultIndexImpl(INDEX_NAME,
				storage, transactionManager, keyConverter, null, comparator);

		// no exception here
		indexImpl.open();

		closeDatabase(indexImpl);
	}

	@Test
	public void storageThrowsException() throws Exception
	{
		final Exception expected = new HGException(
				"While attempting to open index ;sample_index': java.lang.IllegalStateException: This exception is thrown by fake storage.");

		startupEnvironment();
		EasyMock.expect(storage.getConfiguration()).andReturn(new BJEConfig());
		EasyMock.expect(storage.getBerkleyEnvironment()).andThrow(
				new IllegalStateException(
						"This exception is thrown by fake storage."));
		PowerMock.replayAll();
		final DefaultIndexImpl indexImpl = new DefaultIndexImpl(INDEX_NAME,
				storage, transactionManager, keyConverter, valueConverter,
				comparator);

		try
		{
			indexImpl.open();
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
		}
	}
}
