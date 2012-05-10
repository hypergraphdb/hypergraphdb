package hgtest.tx;

import org.hypergraphdb.HGQuery.hg;

import org.testng.Assert;
import org.testng.annotations.Test;

import hgtest.HGTestBase;
import hgtest.beans.SimpleBean;

public class WriteTxTests extends HGTestBase
{
    private int count = 1000;
    
    @Test
    public void testBulkWrite()
    {
        long starttime = System.currentTimeMillis();
        graph.getTransactionManager().beginTransaction();
        for (int i = 0; i < count; i++)
        {
            SimpleBean bean = new SimpleBean();
            bean.setByteProp(Double.valueOf(Math.random()).byteValue());
            bean.setFloatProp(Double.valueOf(Math.random()).floatValue());
            bean.setIntProp(Double.valueOf(Math.random()).intValue());
            bean.setLongProp(Double.valueOf(Math.random()).longValue());
            bean.setDoubleProp(Double.valueOf(Math.random()).doubleValue());
            bean.setShortProp(Double.valueOf(Math.random()).shortValue());
            bean.setStrProp(Double.valueOf(Math.random()).toString());
            bean.setCharProp(Double.valueOf(Math.random()).toString().charAt(0));
            graph.add(bean);
        }
        graph.getTransactionManager().commit();
        System.out.println("time=" + (System.currentTimeMillis() - starttime));
        
        Assert.assertEquals(hg.count(graph, hg.type(SimpleBean.class)), count);
//        for (SimpleBean x : (List<SimpleBean>)(List<?>)hg.getAll(graph, hg.type(SimpleBean.class)))
//            System.out.println(x);
    }
}
