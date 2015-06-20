package hgtest.storage.bje.DefaultBiIndexImpl;

import org.easymock.EasyMock;
import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.bje.BJEConfig;
import org.hypergraphdb.storage.bje.DefaultBiIndexImpl;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import static hgtest.storage.bje.TestUtils.assertExceptions;


/**
 * @author Yuriy Sechko
 */
public class DefaultBiIndexImpl_openTest extends DefaultBiIndexImplTestBasis
{
	@Test
	public void indexNameIsNull() throws Exception
	{
		mockStorage();
		PowerMock.replayAll();

		final DefaultBiIndexImpl indexImpl = new DefaultBiIndexImpl(null,
				storage, transactionManager, keyConverter, valueConverter,
				comparator);
		// there is not exception here
		// just because (DB_NAME_PREFIX + name) becomes "hgstore_idx_null";
		indexImpl.open();

		closeDatabases(indexImpl);
	}

	@Test
	public void storageIsNull() throws Exception
	{
		final HGException expected = new HGException(
				"While attempting to open index ;sample_index': java.lang.NullPointerException");
		PowerMock.replayAll();

		final DefaultBiIndexImpl indexImpl = new DefaultBiIndexImpl(INDEX_NAME,
				null, transactionManager, keyConverter, valueConverter,
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

	@Test
	public void transactionManagerIsNull() throws Exception
	{
		mockStorage();
		PowerMock.replayAll();

		final DefaultBiIndexImpl indexImpl = new DefaultBiIndexImpl(INDEX_NAME,
				storage, null, keyConverter, valueConverter, comparator);
		// no exception here, transactionManager is not used
		// exactly in the open() method
		indexImpl.open();

		closeDatabases(indexImpl);
	}

	@Test
	public void keyConverterIsNull() throws Exception
	{
		mockStorage();
		PowerMock.replayAll();

		final DefaultBiIndexImpl indexImpl = new DefaultBiIndexImpl(INDEX_NAME,
				storage, transactionManager, null, valueConverter, comparator);
		// no exception here, keyConverter is not used
		// exactly in the open() method
		indexImpl.open();

		closeDatabases(indexImpl);
	}

	@Test
	public void valueConverterIsNull() throws Exception
	{
		mockStorage();
		PowerMock.replayAll();

		final DefaultBiIndexImpl indexImpl = new DefaultBiIndexImpl(INDEX_NAME,
				storage, transactionManager, keyConverter, null, comparator);
		// no exception here, similar to the case above
		indexImpl.open();

		closeDatabases(indexImpl);
	}

	@Test
	public void comparatorIsNull() throws Exception
	{
		mockStorage();
		PowerMock.replayAll();

		final DefaultBiIndexImpl indexImpl = new DefaultBiIndexImpl(INDEX_NAME,
				storage, transactionManager, keyConverter, valueConverter,
				comparator);
		// no exception here, similar to the case above
		indexImpl.open();

		closeDatabases(indexImpl);
	}

	@Test
	public void storageThrowsException() throws Exception
	{
		final HGException expected = new HGException(
				"While attempting to open index ;sample_index': java.lang.IllegalStateException");

		EasyMock.expect(storage.getConfiguration()).andReturn(new BJEConfig());
		EasyMock.expect(storage.getBerkleyEnvironment()).andReturn(environment);
		EasyMock.expect(storage.getBerkleyEnvironment()).andThrow(
				new IllegalStateException());
		PowerMock.replayAll();

		final DefaultBiIndexImpl indexImpl = new DefaultBiIndexImpl(INDEX_NAME,
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
		finally
		{
			closeDatabases(indexImpl);
		}

	}
}
