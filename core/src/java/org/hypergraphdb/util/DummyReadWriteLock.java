package org.hypergraphdb.util;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * <p>
 * A read-write lock that doesn't lock at all.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class DummyReadWriteLock implements ReadWriteLock
{
    private Lock dummyLock = new Lock() 
    {
        public void lock() { }
        public void lockInterruptibly() throws InterruptedException { }

        public Condition newCondition() { return null; }

        public boolean tryLock() { return true; }        
        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException
        {
            return true;
        }
        public void unlock() { }
    };

    public Lock readLock()
    {
        return dummyLock;
    }

    public Lock writeLock()
    {
        return dummyLock;
    }
}