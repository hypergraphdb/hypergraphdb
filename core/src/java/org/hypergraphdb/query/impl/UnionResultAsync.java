package org.hypergraphdb.query.impl;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.Future;

import org.hypergraphdb.HGException;
import org.hypergraphdb.util.AggregateFuture;
import org.hypergraphdb.util.CompletedFuture;
import org.hypergraphdb.util.MappedFuture;
import org.hypergraphdb.util.Mapping;

public class UnionResultAsync<T> implements AsyncSearchResult<T>
{
    
    private AsyncSearchResult<T> left, right, last_choice;
    private boolean rightEOF = false, leftEOF = false, rightBOF = false, leftBOF = false;
    private boolean move_both = true;

    // Select the next element and set the state
    // variables move_both and last_choice based on the selection
    private T select(Comparable<T> L, T R)
    {
        int comp = L.compareTo(R);
        if (comp == 0)
        {
            last_choice = left;
            move_both = true;
            return (T)L;
        }
        else if (comp < 0)
        {
            last_choice = left;
            move_both = false;
            return (T)L;
        }
        else
        {
            last_choice = right;
            move_both = false;
            return R;
        }       
    }
    
    private T selectBack(Comparable<T> L, T R)
    {
        int comp = L.compareTo(R);
        if (comp == 0)
        {
            last_choice = left;
            move_both = true;
            return (T)L;
        }
        else if (comp < 0)
        {
            last_choice = right;
            move_both = false;
            return R;
        }
        else
        {
            last_choice = left;
            move_both = false;
            return (T)L;
        }       
    }
    
    public UnionResultAsync(AsyncSearchResult<T> left, AsyncSearchResult<T> right)
    {
        this.left = left;
        this.right = right;
    }
    
    
    public Future<T> current()
    {
        if (last_choice == null)
            throw new NoSuchElementException();
        else
            return last_choice.current();
    }
    
    public Future<Boolean> hasNextAsync()
    {
        AggregateFuture<Boolean> both = new AggregateFuture<Boolean>(left.hasNextAsync(), right.hasNextAsync());
        Mapping<List<Boolean>, Boolean> map = new Mapping<List<Boolean>, Boolean>(){
          public Boolean eval(List<Boolean> L)
          {
              boolean l = L.get(0);
              boolean r = L.get(1);              
              if (l)
                  return true;
              else if (r)
                  return true; 
              else if (last_choice == null)
                  return false;
              // if we picked left last time, we could still have the "current"
              // of the right set remaining to be returned as the last element
              else if (last_choice == left && !rightEOF)
                  try { return ((Comparable)left.current().get()).compareTo(right.current().get()) < 0; }
                  catch (Exception ex) { throw new HGException(ex); }
              else if (last_choice == right && !leftEOF)
                  try { return ((Comparable)right.current().get()).compareTo(left.current().get()) < 0; }
                  catch (Exception ex) { throw new HGException(ex); }
              else return false;              
          }
        };        
        return new MappedFuture<List<Boolean>, Boolean>(both, map);
    }

    
    public Future<Boolean> hasPrevAsync()
    {
        AggregateFuture<Boolean> both = new AggregateFuture<Boolean>(left.hasPrevAsync(), right.hasPrevAsync());
        Mapping<List<Boolean>, Boolean> map = new Mapping<List<Boolean>, Boolean>(){
          public Boolean eval(List<Boolean> L)
          {
              boolean l = L.get(0);
              boolean r = L.get(1);              
              if (l)
                  return true;
              else if (r)
                  return true;
              else if (last_choice == null)
                  return false;
              else if (last_choice == left && !rightBOF)
                  try { return ((Comparable)left.current().get()).compareTo(right.current().get()) > 0; }
                  catch (Exception ex) { throw new HGException(ex); }
              else if (last_choice == right && !leftBOF)
                  try { return ((Comparable)right.current().get()).compareTo(left.current().get()) > 0; }
                  catch (Exception ex) { throw new HGException(ex); }
              else return false;              
          }
        };        
        return new MappedFuture<List<Boolean>, Boolean>(both, map);
    }

    public Future<T> prev()
    {
        if (!hasNext())
            throw new NoSuchElementException();

        AggregateFuture<T> both = new AggregateFuture<T>(left.prev(), right.prev());
        Mapping<List<T>, T> map = new Mapping<List<T>, T>(){
          public T eval(List<T> L)
          {
              T l = L.get(0);
              T r = L.get(1);
              
              if (move_both)
              {
                  if (left.hasPrev())
                      if (!right.hasPrev())
                      {
                          last_choice = left;
                          move_both = false;
                          rightBOF = true;
                          return l;
                      }
                      else
                          return selectBack((Comparable<T>)l, r);
                  else
                  {
                      last_choice = right;
                      move_both = false;  
                      leftBOF = true;
                      return r;
                  }
              }
              else if (last_choice == left)
              {
                  if (!left.hasPrev())
                  {
                      last_choice = right;
                      move_both = false;
                      leftBOF = true;
                      try { return right.current().get(); }
                      catch (Exception ex) { throw new HGException(ex); }
                  }
                  else
                      if (rightBOF) return l;
                      else 
                          try { return selectBack((Comparable<T>)l, right.current().get()); }
                          catch (Exception ex) { throw new HGException(ex); }
              }
              else if (last_choice == right)
              {
                  if (!right.hasPrev())
                  {
                      last_choice = left;
                      move_both = false;
                      rightBOF = true;
                      try { return left.current().get(); }
                      catch (Exception ex) { throw new HGException(ex); }
                  }
                  else
                      if (leftBOF) return r;
                      else 
                          try { return selectBack((Comparable<T>)left.current().get(), r); }
                          catch (Exception ex) { throw new HGException(ex); }
              }
              else
                  throw new NoSuchElementException("This should never be thrown from here!!!"); // we'll never get here       
          }
        };
        return new MappedFuture<List<T>, T>(both, map);            
    }
     
    public Future<T> next()
    {
        if (!hasNext())
            throw new NoSuchElementException();

        final boolean leftHasNext = left.hasNext();
        final boolean rightHasNext = right.hasNext();
        
        Future<T> leftFuture = CompletedFuture.getNull(), rightFuture = CompletedFuture.getNull();
        
        if (leftHasNext && (move_both || last_choice == left))
            leftFuture = left.next();
        
        if (rightHasNext && (move_both || last_choice == right))
            rightFuture = right.next();
        
        AggregateFuture<T> both = new AggregateFuture<T>(leftFuture, rightFuture);
        
        Mapping<List<T>, T> map = new Mapping<List<T>, T>(){
          public T eval(List<T> L)
          {
              T l = L.get(0);
              T r = L.get(1);
              
              if (move_both)
              {
                  if (leftHasNext)
                      if (!rightHasNext)
                      {
                          last_choice = left;
                          move_both = false;
                          rightEOF = true;
                          return l;
                      }
                      else
                          return select((Comparable<T>)l,r);
                  else
                  {
                      last_choice = right;
                      move_both = false;
                      leftEOF = true;
                      return r;
                  }
              }
              else if (last_choice == left)
              {
                  if (!leftHasNext)
                  {
                      last_choice = right;
                      move_both = false;
                      leftEOF = true;
                      try { return right.current().get(); }
                      catch (Exception ex) { throw new HGException(ex); }
                  }
                  else
                      if (rightEOF) return l;
                      else 
                          try { return select((Comparable<T>)l, right.current().get()); }
                          catch (Exception ex) { throw new HGException(ex); }
              }
              else if (last_choice == right)
              {
                  if (!rightHasNext)
                  {
                      last_choice = left;
                      move_both = false;
                      rightEOF = true;
                      try { return left.current().get(); }
                      catch (Exception ex) { throw new HGException(ex); }
                  }
                  else
                      if (leftEOF) return r;
                      else try { return select((Comparable<T>)left.current().get(), r); }
                      catch (Exception ex) { throw new HGException(ex); }
              }
              else
                  throw new NoSuchElementException("This should never be thrown from here!!!"); // we'll never get here              
          }
        };
        return new MappedFuture<List<T>, T>(both, map);            
    }

    public void close()
    {
        left.close();
        right.close();
    }
    
    public boolean hasPrev()
    {
        try
        {
            return hasPrevAsync().get();
        }
        catch (Exception ex)
        {
            throw new HGException(ex);
        }
    }

    
    public boolean hasNext()
    {
        try
        {
            return hasNextAsync().get();
        }
        catch (Exception ex)
        {
            throw new HGException(ex);
        }
    }

    public void remove() 
    {
        throw new UnsupportedOperationException();
    }
    
    public boolean isOrdered()
    {
        return true;
    }
}
    