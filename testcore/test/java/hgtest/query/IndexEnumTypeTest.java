package hgtest.query;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.indexing.ByPartIndexer;
import org.hypergraphdb.indexing.HGIndexer;
import org.hypergraphdb.query.HGQueryCondition;
import org.hypergraphdb.query.cond2qry.ExpressionBasedQuery;
import org.hypergraphdb.query.impl.IndexBasedQuery;
import org.junit.Assert;
import org.junit.Test;

import hgtest.HGTestBase;
import hgtest.beans.AnEnum;
import hgtest.beans.BeanWithEnum;

public class IndexEnumTypeTest extends HGTestBase
{
//    public static void main(String[] args)
//    {
//        IndexEnumTypeTest t = new IndexEnumTypeTest();
//        //t.setUp();
//        t.graph = HGEnvironment.get(t.getGraphLocation());
//        t.testEnumIndex();
//        //t.tearDown();
//        t.graph.close();
//    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testEnumIndex()
    {
        HGIndexer indexer = new ByPartIndexer(graph.getTypeSystem().getTypeHandle(BeanWithEnum.class), "which");
        graph.getIndexManager().register(indexer);
        BeanWithEnum x1 = new BeanWithEnum();
        BeanWithEnum x2 = new BeanWithEnum();
        x2.setWhich(AnEnum.third);
        BeanWithEnum x3 = new BeanWithEnum();
        x3.setWhich(AnEnum.first);
        BeanWithEnum x4 = new BeanWithEnum();
        x4.setWhich(AnEnum.second);
        graph.add(x1);
        graph.add(x2);
        graph.add(x3);
        HGHandle h4 = graph.add(x4);

        HGQueryCondition condition = hg.and(hg.type(BeanWithEnum.class), hg.eq("which", AnEnum.second));
        HGQuery query = HGQuery.make(graph, condition);
//        System.out.println(query.getClass().toString());
//        System.out.println(((ExpressionBasedQuery)query).getCompiledQuery().getClass().toString());
        Assert.assertTrue(query instanceof IndexBasedQuery || 
                          (query instanceof ExpressionBasedQuery &&
                           ((ExpressionBasedQuery)query).getCompiledQuery() instanceof IndexBasedQuery));
        Assert.assertEquals(hg.findOne(graph, condition), h4);
        this.reopenDb();
    }
}
