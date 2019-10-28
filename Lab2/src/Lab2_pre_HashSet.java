import java.util.*;
import java.io.*;
public class Lab2_pre_HashSet {
	public static void main(String[] args)
	{
		HashSet<String> a = new HashSet();
		int n;
		Scanner sc = new Scanner(System.in);
		n = sc.nextInt();
		for(int i=0;i<n;i++)
		{
			a.add(sc.next());
		}
		Iterator i = a.iterator();
		while(i.hasNext())
		{
			System.out.println(i.next());
		}
	}
}
