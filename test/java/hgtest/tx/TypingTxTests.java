package hgtest.tx;

import org.hypergraphdb.HGHandle;
import static org.testng.Assert.*;
import org.testng.annotations.Test;

import hgtest.HGTestBase;
import hgtest.SimpleBean;

public class TypingTxTests extends HGTestBase
{
    @Test
    public void testTypeAbort()
    {
        HGHandle h = graph.getTypeSystem().getTypeHandleIfDefined(SimpleBean.class);
        assertNull(h);
        graph.getTransactionManager().beginTransaction();
        h = graph.getTypeSystem().getTypeHandle(SimpleBean.class);
        assertNotNull(h, null);
        graph.getTransactionManager().abort();
        h = graph.getTypeSystem().getTypeHandleIfDefined(SimpleBean.class);
        assertNull(h);
        reopenDb();
        h = graph.getTypeSystem().getTypeHandleIfDefined(SimpleBean.class);
        assertNull(h);        
    }
}