package Nonbuffer;

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
		Map<Character,ArrayList<Integer>> resultSofar;
		//1 from
		Set <Character> froms = new HashSet<>(Arrays.asList(inputtest.froms));
		Loader[] from = new Loader[inputtest.froms.length];
		int i=0;
		for(Loader l:tables) 
			if(froms.contains(l.name))
				from[i++] = l;
		//2 join
		resultSofar = join.cons(inputtest.wheres, from);
		joins = new HashSet<>(resultSofar.keySet());
		//3 non-join
		resultSofar = nonjoin.cons(inputtest.lastline,tables,resultSofar,joins);
		//4 sum
		output = Sum.cons(resultSofar, inputtest.sums,tables,joins);
	}
	
	public String getResult() {
		return output;
	}
}
