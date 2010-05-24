package org.hypergraphdb.type.javaprimitive;

import java.util.Calendar;

public class CalendarType extends DateTypeBase<java.util.Calendar>
{
    protected Calendar fromLong(long x)
    {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(x);
        return cal;
    }

    protected long toLong(Calendar x)
    {
        return x.getTimeInMillis();
    }
}