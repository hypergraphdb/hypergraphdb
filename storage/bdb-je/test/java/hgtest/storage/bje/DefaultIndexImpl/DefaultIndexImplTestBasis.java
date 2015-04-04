package hgtest.storage.bje.DefaultIndexImpl;

import com.google.code.multitester.annonations.Exported;
import hgtest.storage.bje.IndexImplTestBasis;
import org.easymock.EasyMock;
import org.hypergraphdb.storage.bje.DefaultIndexImpl;
import org.hypergraphdb.transaction.HGTransactionManager;
import org.powermock.api.easymock.PowerMock;

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
        PowerMock.replayAll();
        index = new DefaultIndexImpl<Integer, String>(INDEX_NAME, storage,
                transactionManager, keyConverter, valueConverter, comparator);
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
        PowerMock.replayAll();
        index = new DefaultIndexImpl<Integer, String>(
                INDEX_NAME, storage, fakeTransactionManager, keyConverter,
                valueConverter, comparator);
        index.open();
    }
}
