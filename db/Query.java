package db;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

public class Query {
	
	Loader[] tables;
	Map<Character,ArrayList<Integer>> resultSofar;
	Input inputtest;
	Loader[] from;
	String output;
	HashSet<Character> joins;
	
	public Query(Scanner input,Loader[] tables) throws Exception {
		this.tables = tables;
		inputtest = new Input(input);
		DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream("jointemp.dat")));
		dos.flush();
		dos.close();
		//1 from
		Set <Character> froms = new HashSet<>(Arrays.asList(inputtest.froms));
		Loader[] from = new Loader[inputtest.froms.length];
		int i=0;
		for(Loader l:tables) 
			if(froms.contains(l.name))
				from[i++] = l;
		Map<Character, ArrayList<Integer>> nonjoinresult = nonjoin.cons(inputtest.lastline, tables);
		//2 join
		Joinresult result = join.cons(nonjoinresult,inputtest.wheres,from);
		joins = new HashSet<>(result.name);
		//3 non-join
		//nonjoin nonj = new nonjoin(inputtest.lastline,from,result,joins);
		
		//4 sum
		output = Sum.cons(nonjoinresult,result,inputtest.sums,tables,joins);
	}
	
	public String getResult() {
		return output;
	}
}
