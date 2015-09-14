package org.hypergraphdb.util;

import java.util.Iterator;

@SuppressWarnings("unchecked")
public class Cons<E> extends Pair<E, Cons<E>> implements Iterable<E>
{
    private static final long serialVersionUID = 827177555442759656L;

    @SuppressWarnings("rawtypes")
	public static final Cons EMPTY = new Cons(null);

    public Cons(E e)
    {
        this(e, EMPTY);
    }

    public Cons(E e, Cons<E> rest)
    {
        super(e, rest);
    }
    
    public Cons<E> cons(E x) 
    {
        return new Cons<E>(x, this);
    }
    
    public Cons<E> reverse()
    {
        return reverseInto((Cons<E>) EMPTY);
    }

    public Cons<E> reverseInto(Cons<E> tail)
    {
        Cons<E> result = tail;
        Cons<E> iter = this;
        while (iter != EMPTY)
        {
            result = result.cons(iter.getFirst());
            iter = iter.getSecond();
        }
        return result;
    }

    public Iterator<E> iterator()
    {
        return new Iterator<E>() {
            Cons<E> current = Cons.this;

            public boolean hasNext()
            {
                return current != EMPTY;
            }

            public E next()
            {
                E e = current.getFirst();
                current = current.getSecond();
                return e;
            }

            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

}
