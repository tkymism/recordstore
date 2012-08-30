package com.tkym.labs.integration;

import static com.tkym.labs.integration.SampleTableMeta.ACCOUNT;
import static com.tkym.labs.integration.SampleTableMeta.PAYMENT;
import static com.tkym.labs.integration.SampleTableMeta.PERSON;
import static com.tkym.labs.record.SqliteRecordstoreRepository.MASTER;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tkym.labs.record.Record;
import com.tkym.labs.record.RecordstoreException;
import com.tkym.labs.record.RecordstoreService;
import com.tkym.labs.record.SqliteRecordstoreRepository;


public class SqliteSystemTest {
	private static RecordstoreService service;
	@BeforeClass
	public static void setupClass() throws RecordstoreException{
		service = SqliteRecordstoreRepository.inMemory().create();
	}
	@AfterClass
	public static void teardownClass() throws RecordstoreException{
		service.getTransaction().close();
	}
	@Test
	public void testSystemDataQueryCase001() throws RecordstoreException{
		service.create(PERSON, true);
		service.create(ACCOUNT, true);
		service.create(PAYMENT, true);
		List<Record> list =  service.query(MASTER).record().asList();
		for(Record record : list)
			System.out.println(
					record.get("name")+":"+
					record.get("tbl_name")+":"+
					record.get("type")+":"+
					record.get("rootpage")+":"+
					record.get("sql")
					);
		assertThat(list.size(), is(5));
		assertThat(list.get(0).get("tbl_name",String.class), is(PERSON.tableName())); 
		assertThat(list.get(3).get("tbl_name",String.class), is(ACCOUNT.tableName())); 
		assertThat(list.get(4).get("tbl_name",String.class), is(PAYMENT.tableName())); 
		service.drop(PERSON, true);
		service.drop(ACCOUNT, true);
		service.drop(PAYMENT, true);
	}
}