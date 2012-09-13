package com.tkym.labs.record;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

public class UseStaticsTest {
	@Test
	public void testUseStaticsCase001(){
		UseStatisticsManager manager = new UseStatisticsManager();
		List<UseStatistisCounter<Integer>> list = new ArrayList<UseStatistisCounter<Integer>>();
		for (int i=0; i<100; i++)
			list.add(manager.manage(new Integer(i)));
		for (UseStatistisCounter<Integer> stats : list)
			for (int i=0; i<stats.getSource(); i++)
				stats.use();
		for (UseStatistisCounter<Integer> stats : list)
			assertThat(stats.getMeta().getUseTimes(), is((long)stats.getSource()));
	}
	
	@Test
	public void testUseStaticsCase002_threadSafe() throws InterruptedException{
		final UseStatisticsManager manager = new UseStatisticsManager();
		Thread[] threads = new Thread[100];
		for (int i=0; i<100; i++)
			threads[i] = new Thread(new Runnable() {
				@Override
				public void run() {
					manager.manage(Thread.currentThread().getId());
				}
			});
		for (Thread t : threads) t.start();
		for (Thread t : threads) t.join();
		assertThat(manager.getMaxID(), is(100L));
	}
	
	@Test
	public void testUseStaticsCase003_threadSafe() throws InterruptedException{
		final UseStatisticsManager manager = new UseStatisticsManager();
		Integer source = new Integer(101);
		final UseStatistisCounter<Integer> stats = manager.manage(source);
		Thread[] threads = new Thread[100];
		for (int i=0; i<100; i++)
			threads[i] = new Thread(new Runnable() {
				@Override
				public void run() {
					stats.use();
				}
			});
		for (Thread t : threads) t.start();
		for (Thread t : threads) t.join();
		assertThat(stats.getMeta().getUseTimes(), is(100L));
	}
	
	@Test
	public void testExpirationCase001() throws InterruptedException{
		UseStatisticsManager manager = new UseStatisticsManager();
		List<UseStatistisCounter<Integer>> list = new ArrayList<UseStatistisCounter<Integer>>();
		long time0 = new Date().getTime();
		TimeUnit.MILLISECONDS.sleep(10L);
		for (int i=0; i<100; i++) 
			list.add(manager.manage(new Integer(i)));
		TimeUnit.MILLISECONDS.sleep(10L);
		long time1 = new Date().getTime();
		TimeUnit.MILLISECONDS.sleep(10L);
		for (int i=0; i<100; i++)
			if (i != 32) 
				list.get(i).use();
		TimeUnit.MILLISECONDS.sleep(10L);
		long time2 = new Date().getTime();
		TimeUnit.MILLISECONDS.sleep(10L);
		list.get(77).use();
		TimeUnit.MILLISECONDS.sleep(10L);
		long time3 = new Date().getTime();
		assertThat(manager.expiredSetAt(time0).size(), is(0));
		assertThat(manager.expiredSetAt(time1).size(), is(1));
		assertThat(manager.expiredSetAt(time1).last().getId(), is(32L+1L));
		assertThat(manager.expiredSetAt(time2).size(), is(99));
		assertThat(manager.expiredSetAt(time2).first().getId(), is(32L+1L));
		assertThat(manager.expiredSetAt(time3).size(), is(100));
		assertThat(manager.expiredSetAt(time3).first().getId(), is(32L+1L));
		assertThat(manager.expiredSetAt(time3).last().getId(), is(77L+1L));
	}
	
	@Test
	public void testExpirationCase002() throws InterruptedException{
		UseStatisticsManager manager = new UseStatisticsManager();
		List<UseStatistisCounter<Integer>> list = new ArrayList<UseStatistisCounter<Integer>>();
		long time0 = System.currentTimeMillis();
		TimeUnit.MILLISECONDS.sleep(10L);
		for (int i=0; i<100; i++) 
			list.add(manager.manage(new Integer(i)));
		TimeUnit.MILLISECONDS.sleep(10L);
		long time1 = System.currentTimeMillis();
		TimeUnit.MILLISECONDS.sleep(10L);
		for (int i=0; i<100; i++)
			if (i != 32) 
				list.get(i).use();
		TimeUnit.MILLISECONDS.sleep(10L);
		long time2 = System.currentTimeMillis();
		TimeUnit.MILLISECONDS.sleep(10L);
		list.get(77).use();
		TimeUnit.MILLISECONDS.sleep(10L);
		long dur2 = System.currentTimeMillis() - time2;
		long dur1 = System.currentTimeMillis() - time1;
		long dur0 = System.currentTimeMillis() - time0;
		assertThat(manager.expiredSetFor(dur0, TimeUnit.MILLISECONDS).size(), is(0));
		assertThat(manager.expiredSetFor(dur1, TimeUnit.MILLISECONDS).size(), is(1));
		assertThat(manager.expiredSetFor(dur1, TimeUnit.MILLISECONDS).last().getId(), is(32L+1L));
		assertThat(manager.expiredSetFor(dur2, TimeUnit.MILLISECONDS).size(), is(99));
		assertThat(manager.expiredSetFor(dur2, TimeUnit.MILLISECONDS).first().getId(), is(32L+1L));
		assertThat(manager.expiredSetFor(0L, TimeUnit.MILLISECONDS).size(), is(100));
		assertThat(manager.expiredSetFor(0L, TimeUnit.MILLISECONDS).first().getId(), is(32L+1L));
		assertThat(manager.expiredSetFor(0L, TimeUnit.MILLISECONDS).last().getId(), is(77L+1L));
	}
}