package hgtest.storage.bdb.DefaultBiIndexImpl;

import org.easymock.EasyMock;
import org.hypergraphdb.HGException;
import org.hypergraphdb.storage.bdb.BDBConfig;
import org.hypergraphdb.storage.bdb.DefaultBiIndexImpl;
import org.powermock.api.easymock.PowerMock;
import org.testng.annotations.Test;

import static hgtest.storage.bdb.TestUtils.assertExceptions;


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
		indexImpl.open();

		closeDatabases(indexImpl);
	}

	@Test
	public void storageThrowsException() throws Exception
	{
		final HGException expected = new HGException(
				"While attempting to open index ;sample_index': java.lang.IllegalStateException");

		EasyMock.expect(storage.getConfiguration()).andReturn(new BDBConfig());
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
