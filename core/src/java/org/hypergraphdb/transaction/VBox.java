/*
 * JVSTM: a Java library for Software Transactional Memory
 * Copyright (C) 2005 INESC-ID Software Engineering Group
 * http://www.esw.inesc-id.pt
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 *
 * Author's contact:
 * INESC-ID Software Engineering Group
 * Rua Alves Redol 9
 * 1000 - 029 Lisboa
 * Portugal
 */
package org.hypergraphdb.transaction;

public class VBox<E>
{
    protected HGTransactionManager txManager;
    
    volatile VBoxBody<E> body = makeNewBody(null, 0, null); // is this right?

    /**
     * for sub-classing only....
     */
    protected VBox() { }
    
    public VBox(HGTransactionManager txManager)
    {
        this(txManager, (E) null);
    }

    public VBox(HGTransactionManager txManager, E initial)
    {
        this.txManager = txManager;
        commit(null, initial, 0);
        //put(initial);
    }
    
    public E get()
    {
        HGTransaction tx = txManager.getContext().getCurrent();
        return  (tx == null) ? body.value : tx.getBoxValue(this);
    }

    /**
     * <p>
     * Same as <code>get</code>, except the value is not marked is being read.
     * Thus, if a transaction only writes to a value, that won't conflict with any
     * other transactions. Calling this method makes sense for aggregate structures
     * (collections or records) that are only written to.
     * </p>  
     */
//    public E getForWrite()
//    {
//        HGTransaction tx = txManager.getContext().getCurrent();
//        if (tx == null) 
//            return body.value;
//        {
//            E value = tx.getLocalValue(this);
//            if  (value == null)
//                value = body.getBody(tx.getNumber()).value; 
//            return (value == HGTransaction.NULL_VALUE) ? null : value;            
//        }
//    }
    
    public void put(E newE)
    {
        HGTransaction tx = txManager.getContext().getCurrent();
        if (tx == null)
        {
            // Outside a transaction, just write the latest value: responsibility of the caller
            // that they know what they're doing here.
            txManager.COMMIT_LOCK.lock();
//            try
//            {
                commit(tx, newE, txManager.mostRecentRecord.transactionNumber);
//            }
//            catch (Throwable t)
//            {
//                System.err.println("OOOPS, exception ehere");
//                System.exit(-1);
//            }
//            finally
//            {
                txManager.COMMIT_LOCK.unlock();
//            }
        }        
        else
        {
            tx.setBoxValue(this, newE);
        }
    }

    public VBoxBody<E> commit(HGTransaction tx, E newValue, long txNumber)
    {
        VBoxBody<E> newBody = makeNewBody(newValue, txNumber, this.body);
        this.body = (VBoxBody<E>)newBody;
        return newBody;
    }

    public VBoxBody<E> makeNewBody(E value, long version, VBoxBody<E> next)
    {
//        if (next != null && version > 0 && version == next.version)
//        {
//            System.err.println("oops: new body with same version...");
//        }
        return new VBoxBody<E>(value, version, next);
    }
    
    public void finish(HGTransaction tx)
    {        
    }
}