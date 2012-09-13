package com.tkym.labs.record;

import java.util.Comparator;
import java.util.Date;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

class UseStatisticsManager{
	static final Comparator<UseStatisticsMeta> LAST_USE_COMPARATOR =
			new Comparator<UseStatisticsMeta>() {
				@Override
				public int compare(UseStatisticsMeta o1, UseStatisticsMeta o2) {
					int ret = compare(
							o1.getLastUse().getTime(), 
							o2.getLastUse().getTime());
					if (ret != 0)
						return ret;
					else
						return compare(o1.getId(), o2.getId());
				}
				private int compare(long l1, long l2){
					if (l1 == l2) return 0;
					else if(l1 > l2) return 1;
					else return -1;
				}
			};
	private AtomicLong maxId = new AtomicLong();
	private Map<Long, UseStatisticsMeta> map = 
			new ConcurrentHashMap<Long, UseStatisticsMeta>();
	long getMaxID() {
		return maxId.get();
	}
	<T> UseStatistisCounter<T> manage(T source){
		long id = maxId.incrementAndGet();
		UseStatistisCounter<T> created = new UseStatistisCounter<T>(id, source);
		map.put(id, created.meta);
		return created;
	}
	<T> T unmanage(UseStatistisCounter<T> element){
		T source = element.getSource();
		map.remove(element.getMeta().getId());
		return source;
	}
	Map<Long, UseStatisticsMeta> metaMap(){
		return map;
	}
	TreeSet<UseStatisticsMeta> expiredSetFor(long duration, TimeUnit unit){
		return expiredSetAt(new Date().getTime()-unit.toMillis(duration));
	}
	TreeSet<UseStatisticsMeta> expiredSetAt(Date limit){
		return expiredSetAt(limit.getTime());
	}
	TreeSet<UseStatisticsMeta> expiredSetAt(long limit){
		TreeSet<UseStatisticsMeta> sorter = 
				new TreeSet<UseStatisticsMeta>(LAST_USE_COMPARATOR);
		for (UseStatisticsMeta meta : map.values())
			if (meta.lastUse.getTime() < limit)
				sorter.add(meta);
		return sorter;
	}
}