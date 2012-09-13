package com.tkym.labs.integration;

import static com.tkym.labs.integration.SampleTableMeta.ACCOUNT;
import static com.tkym.labs.integration.SampleTableMeta.EMAIL;
import static com.tkym.labs.integration.SampleTableMeta.PAYMENT;
import static com.tkym.labs.integration.SampleTableMeta.PAY_NO;
import static com.tkym.labs.integration.SampleTableMeta.PERSON;
import static com.tkym.labs.integration.SampleTableMeta.PERSON_ID;
import static com.tkym.labs.record.QueryUtils.and;
import static com.tkym.labs.record.QueryUtils.or;
import static com.tkym.labs.record.QueryUtils.property;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Iterator;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tkym.labs.record.Record;
import com.tkym.labs.record.RecordKey;
import com.tkym.labs.record.RecordstoreException;
import com.tkym.labs.record.RecordstoreService;
import com.tkym.labs.record.RecordstoreServiceFactory;
import com.tkym.labs.record.SqliteRecordstoreServiceFactory;

public class MulitThreadTest {
	private static RecordstoreService service;
	@BeforeClass
	public static void setupClass() throws RecordstoreException {
		RecordstoreServiceFactory factory = new SqliteRecordstoreServiceFactory();
		service = factory.create();
		service.create(PERSON, true);
		service.create(ACCOUNT, true);
		service.create(PAYMENT, true);
	}
	
	@AfterClass
	public static void teardownClass() throws RecordstoreException {
		service.getTransaction().close();
	}
	
	Record user(int id) {
		return SampleTableMeta.user(id, "foo" + id, "bar" + id, 19500101 + id);
	}

	Record account(int id, int emailNo) {
		return SampleTableMeta.account(id, "foo" + id + "@email" + emailNo
				+ ".com", "prop1" + id, "prop2" + id, "prop3" + id);
	}

	Record payment(int id, int emailNo, int payNo) {
		return SampleTableMeta
				.payment(id, "foo" + id + "@email" + emailNo + ".com",
						(short) payNo, 100 * id + 10 * emailNo + payNo, false);
	}
	
	@Test
	public void testCase001() throws RecordstoreException {
		service.put(user(101));
		service.put(account(101, 1));
		service.put(payment(101, 1, 1));
		service.put(payment(101, 1, 2));
		service.put(payment(101, 1, 3));
		service.put(account(101, 2));
		service.put(payment(101, 2, 11));
		service.put(payment(101, 2, 12));
		service.getTransaction().commit();
		service.put(user(102));
		service.put(account(102, 2));
		service.getTransaction().commit();
		
		Iterator<RecordKey> ite = 
				service.
				query(PAYMENT).
				filter(
					and(
						property(PERSON_ID, long.class).equalsTo(101L),
						property(EMAIL, String.class).startsWith("foo"),
						or(
							property(PAY_NO, short.class).equalsTo((short)11),
							property(PAY_NO, short.class).equalsTo((short)12)
						)
					)
				).
				sort(property(PERSON_ID).asc()).
				sort(property(EMAIL).asc()).
				sort(property(PAY_NO).asc()).
				key().asIterator();
		
		while(ite.hasNext()){
			RecordKey key = ite.next();
			Record old_payment = service.get(key);
			Assert.assertNotNull(old_payment);
			Record new_payment = 
					payment(102, 2, old_payment.key().value(PAY_NO, short.class));
			service.put(new_payment);
		}
		service.getTransaction().commit();
		
		Iterator<Record> ite2 = 
				service.
				query(PAYMENT).
				filter(
					and(
						property(PERSON_ID, long.class).equalsTo(102L),
						property(EMAIL, String.class).startsWith("foo"),
						or(
							property(PAY_NO, short.class).equalsTo((short)11),
							property(PAY_NO, short.class).equalsTo((short)12)
						)
					)
				).
				sort(property(PERSON_ID).asc()).
				sort(property(EMAIL).asc()).
				sort(property(PAY_NO).asc()).
				record().asIterator();
		
		int count = 0;
		while(ite2.hasNext()){
			Record record = ite2.next();
			assertThat(record.key().value(PERSON_ID, long.class), is(102L));
			assertThat(record.key().value(EMAIL, String.class), is("foo102@email2.com"));
			assertThat(record.key().value(PAY_NO, short.class), is((short)(11+count)));
			count++;
		}
		assertThat(count, is(2));
	}
}