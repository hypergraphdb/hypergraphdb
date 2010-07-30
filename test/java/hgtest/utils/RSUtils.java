package hgtest.utils;

import org.hypergraphdb.HGSearchResult;

public class RSUtils
{
    public static int countRS(HGSearchResult res, boolean close)
    {
        int i = 0;
        while (res.hasNext())
        {
            res.next();
            i++;
        }
        if (close) res.close();
        return i;
    }
}
