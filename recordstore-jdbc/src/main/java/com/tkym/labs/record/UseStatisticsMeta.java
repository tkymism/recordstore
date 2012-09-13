package com.tkym.labs.record;

import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

class UseStatisticsMeta{
	private final long id;
	private final AtomicLong useTimes;
	Date lastUse;
	private final Date createDate;
	UseStatisticsMeta(long id){
		this.id = id;
		useTimes = new AtomicLong();
		createDate = new Date();
		lastUse = createDate;
	}
	long getId(){
		return id; 
	}
	long getUseTimes(){
		return useTimes.get();
	}
	Date getLastUse(){
		return lastUse;
	}
	void use(){
		useTimes.incrementAndGet();
		lastUse = new Date();
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (id ^ (id >>> 32));
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		UseStatisticsMeta other = (UseStatisticsMeta) obj;
		if (id != other.id)
			return false;
		return true;
	}
}