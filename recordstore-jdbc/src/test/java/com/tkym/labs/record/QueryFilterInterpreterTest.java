package com.tkym.labs.record;

import static com.tkym.labs.record.QueryFilter.QueryFilterOperator.CONTAIN;
import static com.tkym.labs.record.QueryFilter.QueryFilterOperator.END_WITH;
import static com.tkym.labs.record.QueryFilter.QueryFilterOperator.EQUAL;
import static com.tkym.labs.record.QueryFilter.QueryFilterOperator.GREATER_THAN;
import static com.tkym.labs.record.QueryFilter.QueryFilterOperator.GREATER_THAN_OR_EQUAL;
import static com.tkym.labs.record.QueryFilter.QueryFilterOperator.IN;
import static com.tkym.labs.record.QueryFilter.QueryFilterOperator.LESS_THAN;
import static com.tkym.labs.record.QueryFilter.QueryFilterOperator.LESS_THAN_OR_EQUAL;
import static com.tkym.labs.record.QueryFilter.QueryFilterOperator.NOT_EQUAL;
import static com.tkym.labs.record.QueryFilter.QueryFilterOperator.START_WITH;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import com.tkym.labs.record.QueryFilterComposite.QueryFilterCompositeType;
import com.tkym.labs.record.QueryFilterCriteriaInterpreter.PsValue;


import org.junit.Test;


public class QueryFilterInterpreterTest {
	
	@Test
	public void testQueryFilterInterpreter_Case_EQUAL(){
		QueryFilterCriteriaInterpreter interpreter = new QueryFilterCriteriaInterpreter();
		QueryFilter<Long> f1 = new QueryFilter<Long>("id1",EQUAL,1L);
		assertThat(interpreter.statement(f1), is("id1 = ?"));
	}
	
	@Test
	public void testQueryFilterInterpreter_Case_GREATER_THAN(){
		QueryFilterCriteriaInterpreter interpreter = new QueryFilterCriteriaInterpreter();
		QueryFilter<Long> f1 = new QueryFilter<Long>("id1",GREATER_THAN,1L);
		assertThat(interpreter.statement(f1), is("id1 > ?"));
	}
	
	@Test
	public void testQueryFilterInterpreter_Case_GREATER_THAN_OR_EQUAL(){
		QueryFilterCriteriaInterpreter interpreter = new QueryFilterCriteriaInterpreter();
		QueryFilter<Long> f1 = new QueryFilter<Long>("id1",GREATER_THAN_OR_EQUAL,1L);
		assertThat(interpreter.statement(f1), is("id1 >= ?"));
	}
	
	@Test
	public void testQueryFilterInterpreter_Case_LESS_THAN(){
		QueryFilterCriteriaInterpreter interpreter = new QueryFilterCriteriaInterpreter();
		QueryFilter<Long> f1 = new QueryFilter<Long>("id1",LESS_THAN,1L);
		assertThat(interpreter.statement(f1), is("id1 < ?"));
	}
	
	@Test
	public void testQueryFilterInterpreter_Case_LESS_THAN_OR_EQUAL(){
		QueryFilterCriteriaInterpreter interpreter = new QueryFilterCriteriaInterpreter();
		QueryFilter<Long> f1 = new QueryFilter<Long>("id1",LESS_THAN_OR_EQUAL,1L);
		assertThat(interpreter.statement(f1), is("id1 <= ?"));
	}
	
	@Test
	public void testQueryFilterInterpreter_Case_NOT_EQUAL(){
		QueryFilterCriteriaInterpreter interpreter = new QueryFilterCriteriaInterpreter();
		QueryFilter<Long> f1 = new QueryFilter<Long>("id1",NOT_EQUAL,1L);
		assertThat(interpreter.statement(f1), is("id1 <> ?"));
	}
	
	@Test
	public void testQueryFilterInterpreter_Case_IN_001(){
		QueryFilterCriteriaInterpreter interpreter = new QueryFilterCriteriaInterpreter();
		QueryFilter<Long> f1 = new QueryFilter<Long>("id1",IN,1L);
		assertThat(interpreter.statement(f1), is("id1 in (?)"));
	}
	
	@Test
	public void testQueryFilterInterpreter_Case_IN_002(){
		QueryFilterCriteriaInterpreter interpreter = new QueryFilterCriteriaInterpreter();
		QueryFilter<Long> f1 = new QueryFilter<Long>("id1",IN,1L,2L,3L);
		assertThat(interpreter.statement(f1), is("id1 in (?,?,?)"));
	}
	
	@Test
	public void testQueryFilterInterpreter_Case_START_WITH(){
		QueryFilterCriteriaInterpreter interpreter = new QueryFilterCriteriaInterpreter();
		QueryFilter<String> f1 = new QueryFilter<String>("id1",START_WITH,"AAA");
		assertThat(interpreter.statement(f1), is("id1 like ?"));
	}
	
	@Test
	public void testQueryFilterInterpreter_Case_END_WITH(){
		QueryFilterCriteriaInterpreter interpreter = new QueryFilterCriteriaInterpreter();
		QueryFilter<String> f1 = new QueryFilter<String>("id1",END_WITH,"AAA");
		assertThat(interpreter.statement(f1), is("id1 like ?"));
	}
	
	@Test
	public void testQueryFilterInterpreter_Case_CONTAIN(){
		QueryFilterCriteriaInterpreter interpreter = new QueryFilterCriteriaInterpreter();
		QueryFilter<String> f1 = new QueryFilter<String>("id1",CONTAIN,"AAA");
		assertThat(interpreter.statement(f1), is("id1 like ?"));
	}
	
	@Test
	public void testQueryFilterCompositeInterpreter_001(){
		QueryFilterCriteriaInterpreter interpreter = new QueryFilterCriteriaInterpreter();
		QueryFilter<String> f1 = new QueryFilter<String>("id1",IN,"AAA","BBB");
		QueryFilter<String> f2 = new QueryFilter<String>("id2",EQUAL,"AAA");
		QueryFilterComposite c = new QueryFilterComposite(QueryFilterCompositeType.AND, f1, f2);
		assertThat(interpreter.statement(c), is("id1 in (?,?) and id2 = ?"));
	}
	
	@Test
	public void testQueryFilterCompositeInterpreter_002(){
		QueryFilterCriteriaInterpreter interpreter = new QueryFilterCriteriaInterpreter();
		QueryFilter<String> f1 = new QueryFilter<String>("id1",IN,"AAA","BBB");
		QueryFilter<String> f2 = new QueryFilter<String>("id2",EQUAL,"AAA");
		QueryFilter<Integer> f31 = new QueryFilter<Integer>("id3",GREATER_THAN,3);
		QueryFilter<Integer> f32 = new QueryFilter<Integer>("id3",LESS_THAN,4);
		QueryFilterComposite c3 = new QueryFilterComposite(QueryFilterCompositeType.AND, f31, f32);
		QueryFilterComposite c = new QueryFilterComposite(QueryFilterCompositeType.OR, f1, f2, c3);
		assertThat(interpreter.statement(c), is("id1 in (?,?) or id2 = ? or (id3 > ? and id3 < ?)"));
	}
	
	@Test
	public void testQueryFilterCriteriaInterpreter(){
		QueryFilterCriteriaInterpreter interpreter = new QueryFilterCriteriaInterpreter();
		QueryFilter<String> f1 = new QueryFilter<String>("id1",IN,"AAA","BBB");
		QueryFilter<String> f2 = new QueryFilter<String>("id2",EQUAL,"AAA");
		QueryFilter<Integer> f31 = new QueryFilter<Integer>("id3",GREATER_THAN,3);
		QueryFilter<Integer> f32 = new QueryFilter<Integer>("id3",LESS_THAN,4);
		QueryFilterComposite c3 = new QueryFilterComposite(QueryFilterCompositeType.AND, f31, f32);
		QueryFilterCriteria c = new QueryFilterComposite(QueryFilterCompositeType.OR, f1, f2, c3);
		assertThat(interpreter.statement(c), is("id1 in (?,?) or id2 = ? or (id3 > ? and id3 < ?)"));
	}
	
	@Test
	public void testQueryFilterCriteriaPsValue_Case_IN(){
		QueryFilterCriteriaInterpreter interpreter = new QueryFilterCriteriaInterpreter();
		QueryFilter<String> f1 = new QueryFilter<String>("id1",IN,"AAA","BBB");
		assertThat(interpreter.statement(f1), is("id1 in (?,?)"));
		List<PsValue> values = interpreter.psValue(f1);
		assertThat(values.get(0).getValue(), is((Object)"AAA"));
		assertThat(values.get(1).getValue(), is((Object)"BBB"));
		assertThat(values.get(0).getProperty(), is("id1"));
		assertThat(values.get(1).getProperty(), is("id1"));
	}
	
	@Test
	public void testQueryFilterCriteriaPsValue(){
		QueryFilterCriteriaInterpreter interpreter = new QueryFilterCriteriaInterpreter();
		QueryFilter<String> f1 = new QueryFilter<String>("id1", IN,"AAA","BBB");
		QueryFilter<String> f2 = new QueryFilter<String>("id2", EQUAL,"AAA");
		QueryFilter<Integer> f31 = new QueryFilter<Integer>("id3", GREATER_THAN,3);
		QueryFilter<Integer> f32 = new QueryFilter<Integer>("id3", LESS_THAN,4);
		QueryFilterComposite c3 = new QueryFilterComposite(QueryFilterCompositeType.AND, f31, f32);
		QueryFilterCriteria c = new QueryFilterComposite(QueryFilterCompositeType.OR, f1, f2, c3);
		assertThat(interpreter.statement(c), is("id1 in (?,?) or id2 = ? or (id3 > ? and id3 < ?)"));
		List<PsValue> values = interpreter.psValue(c);
		assertThat(values.get(0).getValue(), is((Object)"AAA"));
	}
}