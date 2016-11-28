package hgtest.storage.bje.DefaultIndexImpl;

import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;

import org.hypergraphdb.storage.bje.DefaultIndexImpl;
import org.hypergraphdb.transaction.HGTransactionManager;

import hgtest.storage.bje.IndexImplTestBasis;

public class DefaultIndexImplTestBasis extends IndexImplTestBasis
{

	protected DefaultIndexImpl<Integer, String> index;

	protected void startupIndex()
	{
		mockStorage();
		replay(mockedStorage);
		index = new DefaultIndexImpl<>(INDEX_NAME, mockedStorage,
				transactionManager, keyConverter, valueConverter, comparator,
				null);
		index.open();
	}

	protected void startupIndexWithFakeTransactionManager()
	{
		mockStorage();
		final HGTransactionManager fakeTransactionManager = createStrictMock(HGTransactionManager.class);
		expect(fakeTransactionManager.getContext())
				.andThrow(
						new IllegalStateException(
								"This exception is thrown by fake transaction manager."));
		replay(mockedStorage, fakeTransactionManager);
		index = new DefaultIndexImpl<>(INDEX_NAME, mockedStorage,
				fakeTransactionManager, keyConverter, valueConverter,
				comparator, null);
		index.open();
	}
}
