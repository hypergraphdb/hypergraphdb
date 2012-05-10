package hgtest.query;

public class NestedBean
{
    private InnerBean innerBean;
    
    public InnerBean getInnerBean()
    {
        return innerBean;
    }

    public void setInnerBean(InnerBean innerBean)
    {
        this.innerBean = innerBean;
    }
    
    static NestedBean create(int num)
    {
        return create("prop" + num, num);
    }
    
    static NestedBean create(String prop, int num)
    {
        NestedBean b = new NestedBean();
        b.innerBean = new InnerBean();
        b.innerBean.prop = prop;
        b.innerBean.number = num;
        return b;
    }
    
    @Override
    public String toString()
    {
       return "NB: " + innerBean.toString();
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((innerBean == null) ? 0 : innerBean.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        NestedBean other = (NestedBean) obj;
        if (innerBean == null)
        {
            if (other.innerBean != null) return false;
        }
        else if (!innerBean.equals(other.innerBean)) return false;
        return true;
    }

    public static class InnerBean
    {
        String prop="";
        int number=-1;

        public int getNumber()
        {
            return number;
        }

        public void setNumber(int number)
        {
            this.number = number;
        }

        public String getProp()
        {
            return prop;
        }

        public void setProp(String prop)
        {
            this.prop = prop;
        }

        @Override
        public String toString()
        {
           return prop + ":" + number;
        }
        
        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + number;
            result = prime * result + ((prop == null) ? 0 : prop.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) return true;
            if (obj == null) return false;
            if (getClass() != obj.getClass()) return false;
            InnerBean other = (InnerBean) obj;
            if (number != other.number) return false;
            if (prop == null)
            {
                if (other.prop != null) return false;
            }
            else if (!prop.equals(other.prop)) return false;
            return true;
        }
    }
    
    public static class ExInnerBean1 extends InnerBean
    {
    }
    
    public static class ExInnerBean2 extends InnerBean
    {
    }
    
    public static class ExExInnerBean1 extends ExInnerBean1
    {
    }
    
}
