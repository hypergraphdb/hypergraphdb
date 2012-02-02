package hgtest.types;

/**
 * 
 * <p>
 * When a HGDB type is being created for this class, the system should
 * throw an exception because the bean property 'b' can't be HG-typed.
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class HasNoTypeBean
{
    private NoTypeBean b;

    public HasNoTypeBean()
    {        
    }
    
    public HasNoTypeBean(NoTypeBean b)
    {        
        this.b = b;
    }
    
    public NoTypeBean getB()
    {
        return b;
    }

    public void setB(NoTypeBean b)
    {
        this.b = b;
    }    
}