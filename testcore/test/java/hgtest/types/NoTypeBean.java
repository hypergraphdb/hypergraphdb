package hgtest.types;

/**
 * 
 * <p>
 * The type system should return null when an attempt is made to get a HGDB type for this
 * class, because it has no default constructor.
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class NoTypeBean
{
    private int datum;
    
    public NoTypeBean(int datum)
    {
        this.datum = datum;
    }

    public int getDatum()
    {
        return datum;
    }

    public void setDatum(int datum)
    {
        this.datum = datum;
    }    
}