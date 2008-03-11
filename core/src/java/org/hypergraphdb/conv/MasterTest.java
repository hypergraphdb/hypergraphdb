package org.hypergraphdb.conv;

import static org.objectweb.asm.Opcodes.CHECKCAST;
import static org.objectweb.asm.Opcodes.PUTFIELD;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Container;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.LayoutManager;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.beans.BeanInfo;
import java.beans.EventHandler;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.beans.XMLEncoder;
import java.io.FileOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import javax.imageio.ImageIO;
import javax.swing.AbstractAction;
import javax.swing.Action;
import javax.swing.Box;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JEditorPane;
import javax.swing.JFrame;
import javax.swing.JLayeredPane;
import javax.swing.JList;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JRootPane;
import javax.swing.JTabbedPane;
import javax.swing.JTable;
import javax.swing.KeyStroke;
import javax.swing.plaf.ColorUIResource;
import javax.swing.plaf.InsetsUIResource;
import javax.swing.text.StyledEditorKit;
import javax.swing.text.html.HTMLEditorKit;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.atom.HGRelType;
import org.hypergraphdb.atom.HGStats;
import org.hypergraphdb.conv.types.ClassGenerator;
import org.hypergraphdb.query.AtomTypeCondition;
import org.hypergraphdb.type.BonesOfBeans;
import org.hypergraphdb.type.javaprimitive.IntType;
import org.objectweb.asm.Type;


public class MasterTest
{
	private static final String IMAGE_BASE = "com/kobrix/notebook/images/";
	private static final String PATH = "F:\\kosta\\xpackTest27";
	private static boolean ADD = !true;

	private static ImageIcon makeIcon()
	{
		URL url = null;
		//javax.swing.DefaultBoundedRangeModel
		try
		{
			url = new URL(
					"file:/F:/kosta/ticl/scriba/build/com/kobrix/notebook/images/Undo16.gif");
		}
		catch (Exception ex)
		{
		}
		if (url != null)
		{
			// System.out.println("Adding Icon");
			ImageIcon icon = new ImageIcon(url);
			icon = new ImageIcon(url);
			return icon;
		}
		return null;
	}

	private static JMenuBar makeBar()
	{
		JMenuBar bar = new JMenuBar();
		JMenu menu = new JMenu("File");
		menu.setMnemonic('f');
		JMenuItem mi = new JMenuItem("New");
		mi.addActionListener(new MyActionListener());
		mi.setIcon(makeIcon());
		mi.setToolTipText("New Tooltip");
		mi.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_X,
				ActionEvent.CTRL_MASK));
		menu.add(mi);
		menu.addSeparator();
		Action a = getAction();
		if(a!=null){
		a.putValue(Action.NAME, "Very Long Name Here");
		a.putValue(Action.SMALL_ICON, makeIcon());
		a.putValue(Action.ACCELERATOR_KEY, KeyStroke.getKeyStroke(
				KeyEvent.VK_N, ActionEvent.CTRL_MASK));}
		// mi = new JMenuItem(a);
		mi = new JMenuItem("Very Long Name Here");
		mi.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_N,
				ActionEvent.CTRL_MASK));
		// mi.addActionListener(new MyActionListener());
		// mi.setIcon(makeIcon());
		menu.add(mi);
		// mi.addActionListener((ActionListener)EventHandler.create(
		// ActionListener.class, new MyActionListener(), "openCellTree"));
		bar.add(menu);
		/*
		 * menu = new JMenu("Edit"); a = getAction(); a.putValue(Action.NAME,
		 * "cut"); a.putValue(Action.SMALL_ICON, makeIcon());
		 * a.putValue(Action.ACCELERATOR_KEY, KeyStroke.getKeyStroke(
		 * KeyEvent.VK_X, ActionEvent.CTRL_MASK)); mi = new JMenuItem(a);
		 * //mi.addActionListener(new MyActionListener()); menu.add(mi);
		 */
		// bar.add(menu);
		return bar;
	}

	private static final Action getAction()
	{
		StyledEditorKit kit = new StyledEditorKit();
		Action[] actions = kit.getActions();
		for (Action a : actions)
			if (a.getValue(Action.NAME).equals(StyledEditorKit.cutAction))
				return a;
		return null;
	}

	private static JButton makeButton()
	{
		JButton button = new JButton("test");
		button.setIcon(makeIcon());
		button.addActionListener(new MyActionListener());
		button.setDisplayedMnemonicIndex(0);
		return button;
	}

	private static Component makePanel()
	{
		JPanel panel = new AddRemoveListPanel();//new JPanel();
		//panel.add(makeButton());
		List l = new LinkedList();
		l.add("First"); l.add("Second");
		((AddRemoveListPanel)panel).setData(l);
		//JPanel outer = new JPanel();
		//outer.add(panel);
		//JTabbedPane pane = new JTabbedPane();
	   // pane.addTab("First Pane", panel);	
	   // pane.addTab("Second Pane", new JTable());
		return panel;
		
		//return makeButton();
	}

	private static Object find(HyperGraph hg, Class cls)
	{
		System.out.println("Find: " + cls);
		HGSearchResult res = hg.find(new AtomTypeCondition(cls));
		try
		{
			while (res.hasNext())
			{
				HGHandle hh = (HGHandle) res.next();
				System.out.println("Find-hh: " + hh);
				System.out.println("Find-m: " + hg.get(hh));
				if (hh == null) continue;
				return hg.get(hh);
			}
		}
		finally
		{
			res.close();
		}
		return null;
	}

	public static void main(String[] args)
	{
		//int [] aa = new int[]{};
//		Class t = AddRemoveListPanel.RemoveAction.class;//aa.getClass();//Insets.class;
//		if (t.isPrimitive())
//		{
//			String typeName = BonesOfBeans.wrapperEquivalentOf(t).getName()
//					.replace('.', '/');
//			System.out.println(typeName);
//			
//		} else
//		{
//			System.out.println("NOT: " + t.getName().replace('.', '/'));
//			System.out.println(
//					Type.getType(t).getDescriptor());
//			try{
//			System.out.println(Type
//					.getMethodDescriptor(t.getMethod("setPanel", AddRemoveListPanel.class)));
//			System.out.println(Type
//					.getMethodDescriptor(t.getMethod("getPanel")));
//			
//			}catch(Exception e){
//				
//			}
//			}
//		 if(true) return;
//		
	// Class cl =
	//	JPanel.class;//Container.class;//java.awt.geom.Rectangle2D.class;
		//DefaultConverter c = (DefaultConverter) MetaData.getConverter(cl);
		//System.out.println("Slots: " + c.getSlots() +
	//		":" + c.getSlots().size() + ((DefaultConverter)c).getType());
//		for(Converter.AddOnType a : c.getAllAddOnFields())
//		  System.out.println("All_Addon: " + a.getArgs()[0] + ":" +
//				  a.getTypes()[0]);
//		c = (DefaultConverter) MetaData.getConverter(JRootPane.class);
//		 for(Converter.AddOnType a : c.getAllAddOnFields())
//			  System.out.println("Addon: " + a.getArgs()[0] + ":" +
//					  a.getTypes()[0]);
//		JPanel in = (JPanel)makePanel();
//		Map<String, Object> res1 = c.store(in);
//		System.out.println("Rs:" + in.getBounds() + ": " + res1);
//		LayoutManager mgr = in.getLayout();
//		System.out.println("Rs2:" + in.getLayout());
//		 JFrame ff = new JFrame();
//		 JPanel o = (JPanel) c.make(res1);
//		 ff.getContentPane().add((JPanel) o);
//		 o.setLayout(mgr);
//		 mgr.layoutContainer(o);
//		 
//		 ff.setVisible(true);
//		 Component[] pp = (Component[]) res1.get("component");
//		 System.out.println("Rs:" + o + ":" + pp.length );
		 //if(true) return;
		
		HyperGraph hg = null;
		try
		{
			// listSlots(java.util.Date.class);
			hg = new HyperGraph(PATH);
			// hg.add(new Object[]{"Mist", null, "Most"});
			// hg.add(new Font("Default", 0, 12));
			// hg.add(new Font("Default", 0, 14));
			HGHandle h = null;
			if (ADD)
			{
				
				//h = hg.add(makeBar());
				Component p = makePanel();
				hg.add(p);
				 JFrame frame = new JFrame();
				// frame.setJMenuBar(makeBar());
				 frame.getContentPane().add(p);
				 
				// hg.add(frame);
				
			}
			if (true)
			{
				JFrame f = null;//(JFrame) find(hg, JFrame.class);
				//((JFrame) f).setJMenuBar(makeBar());
				if(f != null)
				{
				
				System.out.println("Frame: " + f.getContentPane().getComponentCount());
				for(Component cc: f.getContentPane().getComponents())
					System.out.println(cc);
				//		+ ":" + ((JRootPane) f.getContentPane().getComponent(0)).getComponentCount());
//				for(Component c: ((JRootPane) f.getContentPane().getComponent(0)).getComponents())
//				{
//					System.out.println("RootPane children: " + c);
//					for(Component inner: ((Container)c).getComponents())
//					{
//						System.out.println("Inner children: " + inner);
//						for(Component inner1: ((Container)inner).getComponents())
//						   System.out.println("Inner1 children: " + inner1);
//						
//					}
//				}
				//((JFrame) f).getContentPane().add(new JButton("Test"), BorderLayout.SOUTH);
				((JFrame) f).validate();
				((JFrame) f).setVisible(true);
				((JFrame) f).validate();
					return;
				}
				Object m = find(hg, JMenuBar.class);
				 JFrame frame = new JFrame();
				 if(m != null)
					frame.setJMenuBar((JMenuBar) m);
				 //else
				//	 frame.setJMenuBar(makeBar());
				 m = find(hg, AddRemoveListPanel.class);
				System.out.println("Panel: " + ((JPanel)m).getComponentCount());
				 if(m!= null)
				   frame.getContentPane().add((Component) m);
				 frame.setVisible(true);
				 if (ADD) hg.add(frame);
//				 XMLEncoder enc = new XMLEncoder(new FileOutputStream(
//						 "E:/temp/Frame.xml"));
//				 enc.writeObject(frame);
//				 enc.close();
//				 enc = new XMLEncoder(new FileOutputStream(
//				 "E:/temp/FBar.xml"));
//		          enc.writeObject(frame.getJMenuBar());
//		          enc.close();
				// find(hg, Font.class);
				// find(hg, MasterTest.TestBean.class);
				// find(hg, ActionListener[].class);
			} else
			{
				Object res = hg.get(hg.getPersistentHandle(h));
				System.out.println("Res: " + res);
				System.out.println("Button: " + ((JPanel) res).getComponent(0));
				// System.out.println("Icon: " + ((JButton)res).getIcon());
			}
		}
		catch (Exception ex)
		{
			ex.printStackTrace();
		}
		finally
		{
			if (hg != null) hg.close();
		}
		// System.exit(0);
	}

	static void test(int number)
	{
		IntType t = new IntType();
		Integer i = new Integer(number);
		byte[] b = t.toByteArray(i);
		int out = (Integer) t.fromByteArray(b);
		if (number != out)
			System.out.println("In: " + number + " Out: " + out);
	}

	private static void listSlots(Class cls) throws IntrospectionException
	{
		// MetaData.getConverter(cls);
		// javax.swing.plaf.FontUIResource
		// javax.swing.DefaultCellEditor
		// javax.swing.plaf.BorderUIResource.BevelBorderUIResource
		BeanInfo i = MetaData.getBeanInfo(cls);
		// BeanInfo i = Introspector.getBeanInfo(cls);
		int c = 0;
		for (PropertyDescriptor pd : i.getPropertyDescriptors())
		{
			Method getter = pd.getReadMethod();
			Method setter = pd.getWriteMethod();
			if (getter != null && setter != null
					&& !DefaultConverter.isTransient(cls, pd))
			{
				System.out.println("PD - accept - " + pd.getName() + ":"
						+ (c++) + ":" + pd.getPropertyType());
			}
			// System.out.println("PD: " + pd.getName() + ":"
			// + pd.getPropertyType());
		}
		Map<String, Class<?>> map1 = MetaData.getConverter(cls).getSlots();
		SortedMap<String, Class<?>> map = new TreeMap<String, Class<?>>(map1);
		for (String s : map.keySet())
			System.out.println(s + ": " + map.get(s).getName());
		System.out.println("map -size: " + map.size());
	}

	public static class MyActionListener implements ActionListener
	{
		public void actionPerformed(ActionEvent e)
		{
			System.out.println("Button pressed");
		}
	}

	private static class TestBean
	{
		List<ActionListener> listeners = new ArrayList<ActionListener>();

		public ActionListener[] getListeners()
		{
			return listeners.toArray(new ActionListener[listeners.size()]);
		}

		// public void setListeners(ActionListener[] listeners) {
		// this.listeners = listeners;
		// }
		public void addListener(ActionListener l)
		{
			listeners.add(l);
		}

		public String toString()
		{
			return "" + listeners.size();
		}

		public void make(TestBean test)
		{
			test.addListener(null);
		}
	}

	public static class TestBeanConverter extends DefaultConverter
	{
		public TestBeanConverter(Class type)
		{
			super(TestBean.class);
		}

		protected void ex_make(Object instance, Map<String, Object> props)
		{
			super.ex_make(instance, props);
			TestBean bean = (TestBean) instance;
			ActionListener[] newChildren = (ActionListener[]) props
					.get("components");
			System.out.println("TestBean - ex_make: " + newChildren);
			if (newChildren != null)
				for (int i = 0; i < newChildren.length; i++)
				{
					bean.addListener(newChildren[i]);
				}
		}

		protected void ex_store(Object instance, Map<String, Object> props)
		{
			// System.out.println("Container - ex_store: " +
			// ((Container)instance).getComponents().length +
			// ":" + instance);
			props.put("components", ((TestBean) instance).getListeners());
		}
		private final Map<String, Class> map = new HashMap<String, Class>();

		protected Map<String, Class> getAuxSlots()
		{
			if (map != null && map.size() == 0)
			{
				map.put("components", ActionListener[].class);
			}
			// System.out.println("Container - getAuxSlots");
			return map;
		}
	}

	static Class getPrivateFieldType(Class declaringClass, String name)
	{
		int dot = name.indexOf(".");
		if (dot > 0)
		{
			Class c = getPrivateFieldType(declaringClass, name
					.substring(0, dot));
			return getPrivateFieldType(c, name.substring(dot + 1));
		}
		try
		{
			Field f = declaringClass.getDeclaredField(name);
			if (!Modifier.isStatic(f.getModifiers()))
			{
				f.setAccessible(true);
				return f.getType();
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return getGetterType(declaringClass, name);
	}

	static Class getGetterType(Class declaringClass, String name)
	{
		try
		{
			Method m = declaringClass.getMethod(getter(name));
			return m.getReturnType();
		}
		catch (Exception ex)
		{
			// System.err.println("" + e + " in " + declaringClass);
			return null;
		}
	}

	static String getter(String s)
	{
		return "get" + s.substring(0, 1).toUpperCase() + s.substring(1);
	}

	static Class getPrivateFieldType1(Class declaringClass, String name)
	{
		try
		{
			Field f = getPublicField(declaringClass, name);
			if (f != null) return f.getType();
			f = declaringClass.getDeclaredField(name);
			if (!Modifier.isStatic(f.getModifiers()))
			{
				f.setAccessible(true);
				return f.getType();
			}
			return f.getType();
		}
		catch (Exception e)
		{
			try
			{
				Method m = declaringClass.getDeclaredMethod(getter(name));
				return m.getReturnType();
			}
			catch (Exception ex)
			{
				System.err.println("" + e + " in " + declaringClass);
			}
		}
		return null;
	}

	static Field getPublicField(Class declaringClass, String name)
	{
		try
		{
			Field f = declaringClass.getField(name);
			if (f != null && !Modifier.isStatic(f.getModifiers())) return f;
		}
		catch (Exception e)
		{
		}
		return null;
	}
}
