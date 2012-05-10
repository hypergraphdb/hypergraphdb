package hgtest.types;

import java.awt.Color;

public class SimpleBean
{
    private int integer = 0;
    private String str = "String";
    private Color color = Color.black;

    public SimpleBean()
    {
    }

    public SimpleBean(int integer, String str, Color color)
    {
        this.integer = integer;
        this.str = str;
        this.color = color;
    }

    public int getInteger()
    {
        return integer;
    }

    public void setInteger(int integer)
    {
        this.integer = integer;
    }

    public String getStr()
    {
        return str;
    }

    public void setStr(String str)
    {
        this.str = str;
    }

    public Color getColor()
    {
        return color;
    }

    public void setColor(Color color)
    {
        this.color = color;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((color == null) ? 0 : color.hashCode());
        result = prime * result + integer;
        result = prime * result + ((str == null) ? 0 : str.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        SimpleBean other = (SimpleBean) obj;
        if (color == null)
        {
            if (other.color != null) return false;
        }
        else if (!color.equals(other.color)) return false;
        if (integer != other.integer) return false;
        if (str == null)
        {
            if (other.str != null) return false;
        }
        else if (!str.equals(other.str)) return false;
        return true;
    }
    
     
}
