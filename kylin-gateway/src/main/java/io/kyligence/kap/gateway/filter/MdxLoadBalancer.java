package io.kyligence.kap.gateway.filter;

import com.netflix.loadbalancer.*;
import io.kyligence.kap.gateway.health.ConcurrentPingStrategy;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MdxLoadBalancer extends BaseLoadBalancer {

	private long mvcc = 0;

	private boolean broken = false;

	private static final int DEFAULT_INTERVAL_SECONDS = 1;

	public MdxLoadBalancer(String name, IPing ping, IRule rule, IPingStrategy pingStrategy, long mvcc) {
		super(name, rule, new LoadBalancerStats(name), null, pingStrategy);
		if (pingStrategy instanceof ConcurrentPingStrategy) {
			setPingInterval(DEFAULT_INTERVAL_SECONDS);
		}

		setPing(ping);
		this.mvcc = mvcc;
	}

	@Override
	public void forceQuickPing() {
		super.forceQuickPing();
	}

	public String getServiceId() {
		return getName();
	}


	public long getMvcc() {
		return mvcc;
	}

	public void setBroken(boolean broken) {
		this.broken = broken;
	}

	public boolean isBroken() {
		return broken;
	}

	@Override
	public String toString() {
		return super.toString();
	}
}
