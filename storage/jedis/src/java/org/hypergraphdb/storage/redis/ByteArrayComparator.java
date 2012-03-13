package org.hypergraphdb.storage.redis;

import java.util.Comparator;

public class ByteArrayComparator implements Comparator<byte[]>
{
    public int compare(byte[] left, byte[] right) {
        if (left == null)                           // handle null in such a way that it is kept at the end of the list.
            return 1;                               // TODO -- what happens if left and right are null? Some endless loop?
        if (right == null)
            return -1;
        for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
            int a = (left[i] & 0xff);
            int b = (right[j] & 0xff);
            if (a != b) {
                return a - b;
            }
        }
        return left.length - right.length;
    }
}
