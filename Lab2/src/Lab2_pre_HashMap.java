import java.util.*;
import java.io.*;
public class Lab2_pre_HashMap {
	public static void main(String[] args)
	{
		Map<Integer, String> a = new HashMap<Integer, String>();
		int n;
		Scanner sc = new Scanner(System.in);
		n = sc.nextInt();
		for(int i=0;i<n;i++)
		{
			int n1 = sc.nextInt();
			String s1 = sc.next();
			//String[] a1 = s.split(" ");
			a.put(n1,s1);			
		}
		Set<Map.Entry<	Integer, String>> values =a.entrySet();
		for(Map.Entry<Integer, String> i : values)
		{
			System.out.println(i.getValue() +" "+ i.getKey());
		}
	}
}