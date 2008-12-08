package org.hypergraphdb.peer.serializer;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Simple JSON parser - copied and adapted from the StringTree library.
 */
public class JSONReader
{
    private static final Object OBJECT_END = new Object();
    private static final Object ARRAY_END = new Object();
    private static final Object COLON = new Object();
    private static final Object COMMA = new Object();
    public static final int FIRST = 0;
    public static final int CURRENT = 1;
    public static final int NEXT = 2;

    private HashMap<Integer, CustomSerializedValue> customValues = new HashMap<Integer, CustomSerializedValue>();
    
    private static Map<Character, Character> escapes = new HashMap<Character, Character>();
    static 
    {
        escapes.put(new Character('"'), new Character('"'));
        escapes.put(new Character('\\'), new Character('\\'));
        escapes.put(new Character('/'), new Character('/'));
        escapes.put(new Character('b'), new Character('\b'));
        escapes.put(new Character('f'), new Character('\f'));
        escapes.put(new Character('n'), new Character('\n'));
        escapes.put(new Character('r'), new Character('\r'));
        escapes.put(new Character('t'), new Character('\t'));
    }

    private CharacterIterator it;
    private char c;
    private Object token;
    private StringBuffer buf = new StringBuffer();

    private char next() 
    {
        c = it.next();
        return c;
    }

    private char previous()
    {
    	c = it.previous();
    	return c;
    }
    
    private void skipWhiteSpace() 
    {
        do
        {
        	if (Character.isWhitespace(c))
        		;
        	else if (c == '/')
        	{
        		next();
        		if (c == '*')
        		{
        			// skip multiline comments
        			while (c != CharacterIterator.DONE)
        				if (next() == '*' && next() == '/')
        						break;
        			if (c == CharacterIterator.DONE)
        				throw new RuntimeException("Unterminated comment while parsing JSON string.");
        		}
        		else if (c == '/')
        			while (c != '\n' && c != CharacterIterator.DONE)
        				next();
        		else
        		{
        			previous();
        			break;
        		}
        	}
        	else
        		break;
        } while (next() != CharacterIterator.DONE);
    }

    public Object read(CharacterIterator ci, int start) 
    {
        it = ci;
        switch (start) 
        {
        	case FIRST:
        		c = it.first();
        		break;
        	case CURRENT:
        		c = it.current();
        		break;
        	case NEXT:
        		c = it.next();
        		break;
        }
        return read();
    }

    public Object read(CharacterIterator it) 
    {
        return read(it, NEXT);
    }

    public Object read(String string) 
    {
        return read(new StringCharacterIterator(string), FIRST);
    }

    private Object read() 
    {
        skipWhiteSpace();
        char ch = c;
        next();
        switch (ch) 
        {
            case '"': token = string(); break;
            case '[': token = array(); break;
            case ']': token = ARRAY_END; break;
            case ',': token = COMMA; break;
            case '{': token = object(); break;
            case '}': token = OBJECT_END; break;
            case ':': token = COLON; break;
            case 't':
                if (c != 'r' || next() != 'u' || next() != 'e')
                	throw new RuntimeException("Invalid JSON token: expected 'true' keyword.");
                next();
                token = Boolean.TRUE;
                break;
            case'f':
                if (c != 'a' || next() != 'l' || next() != 's' || next() != 'e')
                	throw new RuntimeException("Invalid JSON token: expected 'false' keyword.");
                next();
                token = Boolean.FALSE;
                break;
            case 'n':
                if (c != 'u' || next() != 'l' || next() != 'l')
                	throw new RuntimeException("Invalid JSON token: expected 'null' keyword.");
                next();
                token = null;
                break;
            default:
                c = it.previous();
                if (Character.isDigit(c) || c == '-') {
                    token = number();
                }
        }
        // System.out.println("token: " + token); // enable this line to see the token stream
        return token;
    }
    
    private Object object() {
        Map<Object, Object> ret = new HashMap<Object, Object>();
        Object key = read();
        while (token != OBJECT_END) {
            read(); // should be a colon
            if (token != OBJECT_END) {
                ret.put(key, read());
                if (read() == COMMA) {
                    key = read();
                }
            }
        }

        return ret;
    }

    @SuppressWarnings("unchecked")
    private Object array() 
    {
        List<Object> ret = new ArrayList<Object>();
        Object value = read();
        while (token != ARRAY_END) 
        {
            ret.add(value);
            if (read() == COMMA) 
                value = read();
        }
        
        if ((ret.size() == 2) && 
        	(ret.get(0).equals("custom")) && 
        	(ret.get(1) instanceof Map) && ((Map<Integer, CustomSerializedValue>)ret.get(1)).containsKey("pos"))
		{
        	Object pos = ((Map<Integer, CustomSerializedValue>)ret.get(1)).get("pos");
        	if (pos instanceof Long)
        	{
        		CustomSerializedValue customValue = new CustomSerializedValue();
        		customValues.put(((Long)pos).intValue(), customValue);        		
        		return customValue;
        	}
		}
        return ret;
    }

    private Object number() {
        int length = 0;
        boolean isFloatingPoint = false;
        buf.setLength(0);
        
        if (c == '-') {
            add();
        }
        length += addDigits();
        if (c == '.') {
            add();
            length += addDigits();
            isFloatingPoint = true;
        }
        if (c == 'e' || c == 'E') {
            add();
            if (c == '+' || c == '-') {
                add();
            }
            addDigits();
            isFloatingPoint = true;
        }
 
        String s = buf.toString();
        return isFloatingPoint 
            ? (length < 17) ? (Object)Double.valueOf(s) : new BigDecimal(s)
            : (length < 20) ? (Object)Long.valueOf(s) : new BigInteger(s);
    }
 
    private int addDigits() {
        int ret;
        for (ret = 0; Character.isDigit(c); ++ret) {
            add();
        }
        return ret;
    }

    private Object string() {
        buf.setLength(0);
        while (c != '"') {
            if (c == '\\') {
                next();
                if (c == 'u') {
                    add(unicode());
                } else {
                    Object value = escapes.get(new Character(c));
                    if (value != null) {
                        add(((Character) value).charValue());
                    }
                }
            } else {
                add();
            }
        }
        next();

        return buf.toString();
    }

    private void add(char cc) 
    {
        buf.append(cc);
        next();
    }

    private void add() 
    {
        add(c);
    }

    private char unicode() 
    {
        int value = 0;
        for (int i = 0; i < 4; ++i) 
        {
            switch (next()) 
            {
            	case '0': case '1': case '2': case '3': case '4': 
            	case '5': case '6': case '7': case '8': case '9':
            		value = (value << 4) + c - '0';
            		break;
            	case 'a': case 'b': case 'c': case 'd': case 'e': case 'f':
            		value = (value << 4) + (c - 'a') + 10;
            		break;
            	case 'A': case 'B': case 'C': case 'D': case 'E': case 'F':
            		value = (value << 4) + (c - 'A') + 10;
            		break;
            }
        }
        return (char) value;
    }
    
	public HashMap<Integer, CustomSerializedValue> getCustomValues()
	{
		return customValues;
	}    
}