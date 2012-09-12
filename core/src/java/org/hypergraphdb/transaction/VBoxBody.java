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

import java.lang.reflect.Field;

public class VBoxBody<E>
{
    // this static field is used to change the non-static final field "next"
    // see the comments on the clearPrevious method
    private static final Field NEXT_FIELD;

    static
    {
        try
        {
            NEXT_FIELD = VBoxBody.class.getDeclaredField("next");
            NEXT_FIELD.setAccessible(true);
        }
        catch (NoSuchFieldException nsfe)
        {
            throw new Error(
                    "JVSTM error: couldn't get access to the VBoxBody.next field");
        }
    }

    void setNext(VBoxBody<E> value)
    {
        try
        {
            NEXT_FIELD.set(this, value);
        }
        catch (IllegalAccessException iae)
        {
            throw new Error("JVSTM error: cannot set the next field to null");
        }        
    }
    
    public final long version;
    public final VBoxBody<E> next;
    public volatile E value;

    public VBoxBody(E value, long version, VBoxBody<E> next)
    {
        this.version = version;
        this.next = next;
        this.value = value;
    }

    public VBoxBody<E> getBody(long maxVersion)
    {
        VBoxBody<E> b = this;
        while (b.version > maxVersion)
            b = b.next;
        return b;
        // return ((version > maxVersion) ? next.getBody(maxVersion) : this);
    }

    public void clearPrevious()
    {
        // we set the next field to null via reflection because it is
        // a final field

        // making the field final is crucial to ensure that the field
        // is properly initialized (and visible to other threads)
        // after an instance of MultiVersionBoxBody is constructed, as
        // per the new Java Memory Model (JSR133)

        // also, according to the Java specification, we may change a
        // final field only via reflection and in some specific cases
        // (such as object reconstruction after deserialization)

        // even though this use is not the case, the potential
        // problems that may occur do not affect the correcteness of
        // the system: we just want to set the field to null to allow
        // the garbage collector to do its thing...
        setNext(null);
    }
}
