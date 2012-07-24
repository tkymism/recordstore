package com.tkym.labs.record;


import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import com.tkym.labs.record.QuerySorter;
import com.tkym.labs.record.QuerySorterComposite;
import com.tkym.labs.record.QuerySorterCriteria;
import com.tkym.labs.record.QuerySorterCriteriaInterpreter;
import com.tkym.labs.record.QuerySorter.QuerySortDirection;

import org.junit.Test;



public class QuerySorterInterpreterTest {
	@Test
	public void testQuerySorterCriteriaInterpreterCase001(){
		QuerySorterCriteriaInterpreter interpreter = new QuerySorterCriteriaInterpreter();
		QuerySorterCriteria criteria = new QuerySorter("idx1",QuerySortDirection.ASCENDING);
		String actual = interpreter.statement(criteria);
		assertThat(actual, is("idx1"));
	}

	@Test
	public void testQuerySorterCriteriaInterpreterCase002(){
		QuerySorterCriteriaInterpreter interpreter = new QuerySorterCriteriaInterpreter();
		QuerySorterCriteria criteria = new QuerySorter("idx1",QuerySortDirection.DESCENDING);
		String actual = interpreter.statement(criteria);
		assertThat(actual, is("idx1 desc"));
	}
	
	@Test
	public void testQuerySorterCriteriaInterpreterCase003(){
		QuerySorterCriteriaInterpreter interpreter = new QuerySorterCriteriaInterpreter();
		QuerySorter s1 = new QuerySorter("idx1",QuerySortDirection.ASCENDING);
		QuerySorter s2 = new QuerySorter("idx2",QuerySortDirection.DESCENDING);
		QuerySorterCriteria criteria = new QuerySorterComposite(s1, s2); 
		String actual = interpreter.statement(criteria);
		assertThat(actual, is("idx1, idx2 desc"));
	}
}
