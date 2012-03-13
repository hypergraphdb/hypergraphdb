package org.hypergraphdb.transaction;

import redis.clients.jedis.BinaryTransaction;
import redis.clients.jedis.Jedis;

public class JedisStorTransaction implements HGStorageTransaction //extends BinaryTransaction 
{

//	BinaryTransaction bt;
    Jedis jedis;
	
//	public BinaryTransaction getBinaryTransaction()
    public Jedis getBinaryTransaction()
	{
        // return bt;
        return jedis;
    }

    public static JedisStorTransaction nullTransaction()
	{
		return new JedisStorTransaction(null, null);		// TODO - looks dangerous. check it!
	}
	
	public JedisStorTransaction (Jedis jedis, BinaryTransaction bt)
	{
/*		this.bt = bt;
		bt = jedis.multi();  */
        this.jedis = jedis;
	}
	
    public void commit() throws HGTransactionException
    {
//    	bt.exec();
    	
    }
    public void abort() throws HGTransactionException
    {
//    	bt.discard();
    }
}
