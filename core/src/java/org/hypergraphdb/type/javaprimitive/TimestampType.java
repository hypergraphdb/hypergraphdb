package org.hypergraphdb.type.javaprimitive;

import java.sql.Timestamp;

public class TimestampType extends DateTypeBase<java.sql.Timestamp>
{
    protected Timestamp fromLong(long x)
    {
        return new Timestamp(x);
    }

    protected long toLong(Timestamp x)
    {
        return x.getTime();
    }
}