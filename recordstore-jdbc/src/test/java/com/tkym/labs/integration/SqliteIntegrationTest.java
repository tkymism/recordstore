package com.tkym.labs.integration;

import static com.tkym.labs.integration.SampleTableMeta.ACCOUNT;
import static com.tkym.labs.integration.SampleTableMeta.EMAIL;
import static com.tkym.labs.integration.SampleTableMeta.PAYMENT;
import static com.tkym.labs.integration.SampleTableMeta.PAY_NO;
import static com.tkym.labs.integration.SampleTableMeta.PERSON;
import static com.tkym.labs.integration.SampleTableMeta.PERSON_ID;
import static com.tkym.labs.record.QueryFilterUtils.and;
import static com.tkym.labs.record.QueryFilterUtils.property;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Iterator;
import java.util.List;

import org.hamcrest.CoreMatchers;
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
import com.tkym.labs.record.TableMeta;

public class SqliteIntegrationTest {
	private static RecordstoreService source;
	private static RecordstoreService target;

	@BeforeClass
	public static void setupClass() throws RecordstoreException {
		RecordstoreServiceFactory factory = new SqliteRecordstoreServiceFactory();
		source = factory.create();
		target = factory.create();
		source.create(PERSON, true);
		source.create(ACCOUNT, true);
		source.create(PAYMENT, true);
		target.create(PERSON, true);
		target.create(ACCOUNT, true);
		target.create(PAYMENT, true);
	}

	@AfterClass
	public static void teardownClass() throws RecordstoreException {
		source.getTransaction().close();
		target.getTransaction().close();
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

	public void test() {
		TableMeta.table("ACCOUNT"). // Table
				key("PERSON_ID").asLong(). // PrimaryKey
				key("ACCOUNT_ID").asInteger().column("EMAIL").asString(). // PrimaryKey
				column("ACCONT_TYPE").asInteger().meta(); // meta()
	}

	@Test
	public void testReplicationCase001() throws RecordstoreException {
		source.put(user(101));
		source.put(account(101, 1));
		source.put(payment(101, 1, 1));
		source.put(payment(101, 1, 2));
		source.put(payment(101, 1, 3));
		source.put(account(101, 2));
		source.put(payment(101, 2, 11));
		source.put(payment(101, 2, 12));
		source.getTransaction().commit();

		assertThat(source.query(PERSON).record().asList().size(), is(1));
		assertThat(source.query(ACCOUNT).record().asList().size(), is(2));
		assertThat(source.query(PAYMENT).record().asList().size(), is(5));

		Iterator<Record> ite = source.query(PERSON).filter(PERSON_ID)
				.equalsTo(101L).record().asIterator();

		Record user101 = ite.next();
		target.put(user101);

		Iterator<Record> iteAccount = source.query(ACCOUNT).filter(PERSON_ID)
				.equalsTo(user101.key().value(PERSON_ID)).record().asIterator();
		while (iteAccount.hasNext())
			target.put(iteAccount.next());

		Iterator<Record> itePayment = source.query(PAYMENT).filter(PERSON_ID)
				.equalsTo(user101.key().value(PERSON_ID)).record().asIterator();
		while (itePayment.hasNext())
			target.put(itePayment.next());
		target.getTransaction().commit();

		List<Record> users = target.query(PERSON).record().asList();
		List<Record> accounts = target.query(ACCOUNT).record().asList();
		List<Record> payments = target.query(PAYMENT).record().asList();

		assertThat(users.size(), is(1));
		assertThat(accounts.size(), is(2));
		assertThat(payments.size(), is(5));

		for (Record record : users) {
			target.deleteIfExists(record.key());
			source.deleteIfExists(record.key());
		}
		for (Record record : accounts) {
			target.deleteIfExists(record.key());
			source.deleteIfExists(record.key());
		}
		for (Record record : payments) {
			target.deleteIfExists(record.key());
			source.deleteIfExists(record.key());
		}

		assertThat(target.query(PERSON).record().asList().size(), is(0));
		assertThat(target.query(ACCOUNT).record().asList().size(), is(0));
		assertThat(target.query(PAYMENT).record().asList().size(), is(0));
		assertThat(source.query(PERSON).record().asList().size(), is(0));
		assertThat(source.query(ACCOUNT).record().asList().size(), is(0));
		assertThat(source.query(PAYMENT).record().asList().size(), is(0));
	}

	@Test
	public void testReplicationCase002() throws RecordstoreException {
		source.put(user(101));
		source.put(account(101, 1));
		source.put(payment(101, 1, 1));
		source.put(payment(101, 1, 2));
		source.put(payment(101, 1, 3));
		source.put(account(101, 2));
		source.put(payment(101, 2, 11));
		source.put(payment(101, 2, 12));
		source.getTransaction().commit();

		target.put(user(101));
		target.put(account(101, 1));
		target.put(payment(101, 1, 1));
		target.put(payment(101, 1, 2));
		target.put(account(101, 2));
		target.put(payment(101, 2, 11));
		target.put(payment(101, 2, 12));
		target.put(payment(101, 2, 13));
		target.getTransaction().commit();

		assertThat(source.query(PERSON).record().asList().size(), is(1));
		assertThat(source.query(ACCOUNT).record().asList().size(), is(2));
		assertThat(source.query(PAYMENT).record().asList().size(), is(5));
		assertThat(target.query(PERSON).record().asList().size(), is(1));
		assertThat(target.query(ACCOUNT).record().asList().size(), is(2));
		assertThat(target.query(PAYMENT).record().asList().size(), is(5));

		Iterator<Record> ite = source.query(PERSON).filter(PERSON_ID)
				.equalsTo(101L).record().asIterator();

		Record user101 = ite.next();
		target.put(user101);

		// remove
		List<RecordKey> sourceKeyList = source.query(ACCOUNT).filter(PERSON_ID)
				.equalsTo(user101.key().value(PERSON_ID)).key().asList();
		Iterator<RecordKey> targetIterator = target.query(ACCOUNT)
				.filter(PERSON_ID).equalsTo(user101.key().value(PERSON_ID))
				.key().asIterator();
		while (targetIterator.hasNext()) {
			RecordKey toKey = targetIterator.next();
			if (!sourceKeyList.contains(toKey))
				target.deleteIfExists(toKey);
		}

		// remove
		sourceKeyList = source.query(PAYMENT).filter(PERSON_ID)
				.equalsTo(user101.key().value(PERSON_ID)).key().asList();

		targetIterator = target.query(PAYMENT).filter(PERSON_ID)
				.equalsTo(user101.key().value(PERSON_ID)).key().asIterator();

		while (targetIterator.hasNext()) {
			RecordKey toKey = targetIterator.next();
			if (!sourceKeyList.contains(toKey))
				target.deleteIfExists(toKey);
		}

		// put
		Iterator<Record> iteAccount = source.query(ACCOUNT).filter(PERSON_ID)
				.equalsTo(user101.key().value(PERSON_ID)).record().asIterator();
		while (iteAccount.hasNext())
			target.put(iteAccount.next());

		Iterator<Record> itePayment = source.query(PAYMENT).filter(PERSON_ID)
				.equalsTo(user101.key().value(PERSON_ID)).record().asIterator();
		while (itePayment.hasNext())
			target.put(itePayment.next());
		target.getTransaction().commit();

		List<Record> users = target.query(PERSON).sort(PERSON_ID).asc()
				.record().asList();
		List<Record> accounts = target.query(ACCOUNT).sort(PERSON_ID).asc()
				.sort(EMAIL).asc().record().asList();
		List<Record> payments = target.query(PAYMENT).sort(PERSON_ID).asc()
				.sort(EMAIL).asc().sort(PAY_NO).asc().record().asList();

		assertThat(users.size(), is(1));
		assertThat(accounts.size(), is(2));
		assertThat(payments.size(), is(5));
		Assert.assertThat(payments.get(0).key().value(PAY_NO, short.class),
				CoreMatchers.is((short) 1));
		Assert.assertThat(payments.get(1).key().value(PAY_NO, short.class),
				CoreMatchers.is((short) 2));
		Assert.assertThat(payments.get(2).key().value(PAY_NO, short.class),
				CoreMatchers.is((short) 3));
		Assert.assertThat(payments.get(3).key().value(PAY_NO, short.class),
				CoreMatchers.is((short) 11));
		Assert.assertThat(payments.get(4).key().value(PAY_NO, short.class),
				CoreMatchers.is((short) 12));

		for (Record record : users) {
			target.deleteIfExists(record.key());
			source.deleteIfExists(record.key());
		}
		for (Record record : accounts) {
			target.deleteIfExists(record.key());
			source.deleteIfExists(record.key());
		}
		for (Record record : payments) {
			target.deleteIfExists(record.key());
			source.deleteIfExists(record.key());
		}

		assertThat(target.query(PERSON).record().asList().size(), is(0));
		assertThat(target.query(ACCOUNT).record().asList().size(), is(0));
		assertThat(target.query(PAYMENT).record().asList().size(), is(0));
		assertThat(source.query(PERSON).record().asList().size(), is(0));
		assertThat(source.query(ACCOUNT).record().asList().size(), is(0));
		assertThat(source.query(PAYMENT).record().asList().size(), is(0));
	}

	@Test
	public void testReplicationCase003() throws RecordstoreException {
		source.put(user(101));
		source.put(account(101, 1));
		source.put(payment(101, 1, 1));
		source.put(payment(101, 1, 2));
		source.put(payment(101, 1, 3));
		source.put(account(101, 2));
		source.put(payment(101, 2, 11));
		source.put(payment(101, 2, 12));
		source.getTransaction().commit();

		target.put(user(101));
		target.put(account(101, 1));
		target.put(payment(101, 1, 1));
		target.put(payment(101, 1, 2));
		target.put(account(101, 2));
		target.put(payment(101, 2, 11));
		target.put(payment(101, 2, 12));
		target.put(payment(101, 2, 13));
		target.getTransaction().commit();

		assertThat(source.query(PERSON).record().asList().size(), is(1));
		assertThat(source.query(ACCOUNT).record().asList().size(), is(2));
		assertThat(source.query(PAYMENT).record().asList().size(), is(5));
		assertThat(target.query(PERSON).record().asList().size(), is(1));
		assertThat(target.query(ACCOUNT).record().asList().size(), is(2));
		assertThat(target.query(PAYMENT).record().asList().size(), is(5));

		Iterator<Record> ite = 
				source.
				query(PERSON).
				filter(
						and(property(PERSON_ID, long.class).equalsTo(101L)))
				.record().asIterator();

		Record user101 = ite.next();
		target.put(user101);

		List<RecordKey> 
		sourceKeyList = source.query(ACCOUNT).
				filter(
						and(property(PERSON_ID).equalsTo(user101.key().value(PERSON_ID)))
				).key().asList();
		Iterator<RecordKey> targetIterator = target.query(ACCOUNT).
				filter(property(PERSON_ID).equalsTo(user101.key().value(PERSON_ID))).
				key().asIterator();
		while (targetIterator.hasNext()) {
			RecordKey toKey = targetIterator.next();
			if (!sourceKeyList.contains(toKey))
				target.deleteIfExists(toKey);
		}

		// remove
		sourceKeyList = source.query(PAYMENT).
				filter(property(PERSON_ID).equalsTo(user101.key().value(PERSON_ID))).
				key().asList();

		targetIterator = target.query(PAYMENT).
				filter(property(PERSON_ID).equalsTo(user101.key().value(PERSON_ID))).
				key().asIterator();

		while (targetIterator.hasNext()) {
			RecordKey toKey = targetIterator.next();
			if (!sourceKeyList.contains(toKey))
				target.deleteIfExists(toKey);
		}

		// put
		Iterator<Record> iteAccount = source.query(ACCOUNT).filter(PERSON_ID)
				.equalsTo(user101.key().value(PERSON_ID)).record().asIterator();
		while (iteAccount.hasNext())
			target.put(iteAccount.next());

		Iterator<Record> itePayment = source.query(PAYMENT).filter(PERSON_ID)
				.equalsTo(user101.key().value(PERSON_ID)).record().asIterator();
		while (itePayment.hasNext())
			target.put(itePayment.next());
		target.getTransaction().commit();

		List<Record> users = 
				target.query(PERSON).
				sort(PERSON_ID).asc().
				record().asList();
		List<Record> accounts = 
				target.query(ACCOUNT).
				sort(PERSON_ID).asc().
				sort(EMAIL).asc().
				record().asList();
		List<Record> payments = 
				target.query(PAYMENT).
				sort(PERSON_ID).asc().
				sort(EMAIL).asc().
				sort(PAY_NO).asc().
				record().asList();

		assertThat(users.size(), is(1));
		assertThat(accounts.size(), is(2));
		assertThat(payments.size(), is(5));
		Assert.assertThat(payments.get(0).key().value(PAY_NO, short.class),
				CoreMatchers.is((short) 1));
		Assert.assertThat(payments.get(1).key().value(PAY_NO, short.class),
				CoreMatchers.is((short) 2));
		Assert.assertThat(payments.get(2).key().value(PAY_NO, short.class),
				CoreMatchers.is((short) 3));
		Assert.assertThat(payments.get(3).key().value(PAY_NO, short.class),
				CoreMatchers.is((short) 11));
		Assert.assertThat(payments.get(4).key().value(PAY_NO, short.class),
				CoreMatchers.is((short) 12));

		for (Record record : users) {
			target.deleteIfExists(record.key());
			source.deleteIfExists(record.key());
		}
		for (Record record : accounts) {
			target.deleteIfExists(record.key());
			source.deleteIfExists(record.key());
		}
		for (Record record : payments) {
			target.deleteIfExists(record.key());
			source.deleteIfExists(record.key());
		}

		assertThat(target.query(PERSON).record().asList().size(), is(0));
		assertThat(target.query(ACCOUNT).record().asList().size(), is(0));
		assertThat(target.query(PAYMENT).record().asList().size(), is(0));
		assertThat(source.query(PERSON).record().asList().size(), is(0));
		assertThat(source.query(ACCOUNT).record().asList().size(), is(0));
		assertThat(source.query(PAYMENT).record().asList().size(), is(0));
	}
}