package hgtest;

import org.hypergraphdb.HGHandle;

public class AtomOperation
{
    public AtomOperationKind kind;
    public HGHandle atomHandle; // null for add, not null for replace, remove, define
    public HGHandle atomType; // may be null if type can be inferred
    public Object atomValue; // for replace, update
    
    public AtomOperation(AtomOperationKind kind, Object atomValue)
    {
        this.kind = kind;
        this.atomValue = atomValue;
    }
    
    public AtomOperation(AtomOperationKind kind, Object atomValue, HGHandle atomType)
    {
        this.kind = kind;
        this.atomValue = atomValue;
        this.atomType = atomType;
    }    
    
    public AtomOperation(AtomOperationKind kind, HGHandle atomHandle, Object atomValue)
    {
        this.kind = kind;
        this.atomValue = atomValue;
        this.atomHandle = atomHandle;
    }
    
    public AtomOperation(AtomOperationKind kind, HGHandle atomHandle)
    {
        this.kind = kind;
        this.atomHandle = atomHandle;
    }        
    
    public AtomOperation(AtomOperationKind kind, HGHandle atomHandle, Object atomValue, HGHandle atomType)
    {
        this.kind = kind;
        this.atomValue = atomValue;
        this.atomHandle = atomHandle;
        this.atomType = atomType;
    }        
}