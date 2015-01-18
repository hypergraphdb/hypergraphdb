package hgtest.storage.bje.DefaultIndexImpl;

import hgtest.storage.bje.IndexImplTestBasis;
import org.hypergraphdb.storage.bje.DefaultIndexImpl;
import org.powermock.api.easymock.PowerMock;

/**
 * @author Yuriy Sechko
 */
public class DefaultIndexImplTestBasis extends IndexImplTestBasis{
    protected DefaultIndexImpl<Integer, String> index;

    protected void startupIndex()
    {
        mockStorage();
        PowerMock.replayAll();
        index = new DefaultIndexImpl<Integer, String>(INDEX_NAME, storage,
                transactionManager, keyConverter, valueConverter, comparator);
        index.open();
    }
}
