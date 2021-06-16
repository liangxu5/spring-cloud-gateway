package io.kyligence.kap.gateway.route.reader;

import com.google.common.collect.Lists;
import com.netflix.loadbalancer.Server;
import io.kyligence.kap.gateway.config.GlobalConfig;
import io.kyligence.kap.gateway.config.MdxConfig;
import io.kyligence.kap.gateway.constant.KylinGatewayVersion;
import io.kyligence.kap.gateway.entity.KylinRouteRaw;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.stream.Collectors;

public class ConfigRouteTableReader implements IRouteTableReader {

	@Autowired
	private MdxConfig mdxConfig;

	@Override
	public List<KylinRouteRaw> list() {
		List<KylinRouteRaw> kylinRouteRawList = Lists.newArrayList();
		List<MdxConfig.ProxyInfo> proxyInfos = mdxConfig.getProxy();
		for (MdxConfig.ProxyInfo proxyInfo : proxyInfos) {
			if (!KylinGatewayVersion.MDX.equals(proxyInfo.getType())) {
				continue;
			}
			List<Server> servers = proxyInfo.getServers().stream().map(Server::new).collect(Collectors.toList());
			KylinRouteRaw kylinRouteRaw = new KylinRouteRaw(proxyInfo.getType(), proxyInfo.getHost(), servers);
			kylinRouteRawList.add(kylinRouteRaw);
		}
		return kylinRouteRawList;
	}
}
