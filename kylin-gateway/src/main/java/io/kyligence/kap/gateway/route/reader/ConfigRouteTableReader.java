package io.kyligence.kap.gateway.route.reader;

import com.google.common.collect.Lists;
import com.netflix.loadbalancer.Server;
import io.kyligence.kap.gateway.config.GlobalConfig;
import io.kyligence.kap.gateway.config.MdxConfig;
import io.kyligence.kap.gateway.constant.KylinGatewayVersion;
import io.kyligence.kap.gateway.entity.KylinRouteRaw;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
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
			if (proxyInfo.getServers() == null) {
				log.error("The server list is null, please check it!");
				return kylinRouteRawList;
			}
			List<Server> servers = proxyInfo.getServers().stream().map(Server::new).collect(Collectors.toList());
			KylinRouteRaw kylinRouteRaw = new KylinRouteRaw(proxyInfo.getType(), proxyInfo.getHost(), servers);
			kylinRouteRawList.add(kylinRouteRaw);
		}
		return kylinRouteRawList;
	}
}
