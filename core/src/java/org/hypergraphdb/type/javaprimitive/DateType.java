package org.hypergraphdb.type.javaprimitive;

import java.util.Date;

public class DateType extends DateTypeBase<java.util.Date>
{
    protected Date fromLong(long x)
    {
        return new Date(x);
    }

    protected long toLong(Date x)
    {
        return x.getTime();
    }
}
