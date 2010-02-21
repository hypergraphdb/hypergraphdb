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
    
    VBoxBody<E> body = makeNewBody(null, 0, null); // is this right?

    public VBox(HGTransactionManager txManager)
    {
        this(txManager, (E) null);
    }

    public VBox(HGTransactionManager txManager, E initial)
    {
        this.txManager = txManager;
        put(initial);        
    }
    
    public E get()
    {
        HGTransaction tx = txManager.getContext().getCurrent();
        if (tx == null)
        {
            // Outside a transaction just return the latest value.
            return body.value;
        }
        else
        {
            return tx.getBoxValue(this);
        }
    }

    public void put(E newE)
    {
        HGTransaction tx = txManager.getContext().getCurrent();
        if (tx == null)
        {
            // Outside a transaction, just write the latest value: responsibility of the caller
            // that they know what they're doing here.
            txManager.COMMIT_LOCK.lock();
            commit(newE, txManager.mostRecentRecord.transactionNumber);
            txManager.COMMIT_LOCK.unlock();
        }        
        else
        {
            tx.setBoxValue(this, newE);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> VBoxBody<T> commit(T newValue, long txNumber)
    {
        VBoxBody<T> newBody = makeNewBody(newValue, txNumber, (VBoxBody<T>)this.body);
        this.body = (VBoxBody<E>)newBody;
        return newBody;
    }

    // in the future, if more than one subclass of body exists, we may
    // need a factory here but, for now, it's simpler to have it like
    // this
    public static <T> VBoxBody<T> makeNewBody(T value, long version,
                                              VBoxBody<T> next)
    {
        return new VBoxBody<T>(value, version, next);
    }
}