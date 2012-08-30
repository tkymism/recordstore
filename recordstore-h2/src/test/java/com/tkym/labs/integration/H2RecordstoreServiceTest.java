package com.tkym.labs.integration;

import static com.tkym.labs.integration.SampleTableMeta.ACCOUNT;
import static com.tkym.labs.integration.SampleTableMeta.PAYMENT;
import static com.tkym.labs.integration.SampleTableMeta.PERSON;
import static com.tkym.labs.integration.SampleTableMeta.PERSON_ID;
import static com.tkym.labs.integration.SampleTableMeta.account;
import static com.tkym.labs.integration.SampleTableMeta.payment;
import static com.tkym.labs.integration.SampleTableMeta.user;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.text.DecimalFormat;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tkym.labs.record.H2RecordstoreServiceFactory;
import com.tkym.labs.record.RecordKey;
import com.tkym.labs.record.RecordstoreException;
import com.tkym.labs.record.RecordstoreService;

public class H2RecordstoreServiceTest {
	private static RecordstoreService service = null;
	@BeforeClass
	public static void beforeClass() throws RecordstoreException{
		H2RecordstoreServiceFactory factory = 
				  new H2RecordstoreServiceFactory(
						H2RecordstoreServiceTest.class.getResource("db"));
		service = factory.create();
		service.create(PERSON, true);
		service.create(ACCOUNT, true);
		service.create(PAYMENT, true);
	}
	
//	@Test
	public void testRecordstoreServiceCase001() throws RecordstoreException{
		DecimalFormat format = new DecimalFormat("000");
		for(int i=0; i<100; i++){
			service.insert(user(i, "foo"+(i%10), format.format(i), 20120401));
			for (int j=0; j<10; j++){
				String email = format.format(i)+"@"+j+".com";
				service.insert(account(i, email, "", "", ""));
				for (short k=0; k<100; k++)
					service.insert(payment(i, email, k, (k+j*1000), false));
			}
		}
	}
	
	@Test
	public void testRecordstoreServiceCase002() throws RecordstoreException{
		DecimalFormat format = new DecimalFormat("000");
		for(int i=0; i<100; i++)
			service.insert(user(i, "foo"+(i%10), format.format(i), 20120401));
		service.getTransaction().commit();
		List<RecordKey> list = 
			service.query(PERSON).
			filter(PERSON_ID, Long.class).greaterThan(80L).
			filter(PERSON_ID, Long.class).lessEqual(90L).
			key().asList();
		assertThat(list.size(), is(10));
	}
	
	@AfterClass
	public static void afterClass() throws RecordstoreException{
		service.drop(PERSON, false);
		service.drop(ACCOUNT, false);
		service.drop(PAYMENT, false);
	}
}