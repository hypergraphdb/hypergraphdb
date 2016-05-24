package hgtest.storage.bje.DefaultIndexImpl;

import com.google.code.multitester.annonations.Exported;
import hgtest.storage.bje.IndexImplTestBasis;
import org.easymock.EasyMock;
import org.hypergraphdb.storage.bje.DefaultIndexImpl;
import org.hypergraphdb.transaction.HGTransactionManager;
import org.powermock.api.easymock.PowerMock;

import static org.easymock.EasyMock.replay;

/**
 * @author Yuriy Sechko
 */
public class DefaultIndexImplTestBasis extends IndexImplTestBasis{

    @Exported("underTest")
    protected DefaultIndexImpl<Integer, String> index;

    @Exported("up2")
    protected void startupIndex()
    {
        mockStorage();
        replay(mockedStorage);
        index = new DefaultIndexImpl<Integer, String>(INDEX_NAME, mockedStorage,
                transactionManager, keyConverter, valueConverter, comparator, null);
        index.open();
    }

    protected void startupIndexWithFakeTransactionManager() {
        mockStorage();
        final HGTransactionManager fakeTransactionManager = PowerMock
                .createStrictMock(HGTransactionManager.class);
        EasyMock.expect(fakeTransactionManager.getContext())
                .andThrow(
                        new IllegalStateException(
                                "This exception is thrown by fake transaction manager."));
        EasyMock.replay(mockedStorage, fakeTransactionManager);
        index = new DefaultIndexImpl<Integer, String>(
                INDEX_NAME, mockedStorage, fakeTransactionManager, keyConverter,
                valueConverter, comparator, null);
        index.open();
    }
}
