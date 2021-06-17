package io.kyligence.kap.gateway.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.Map;

@Configuration
@Data
@ConfigurationProperties(prefix = "mdx")
public class MdxConfig {

	List<ProxyInfo> proxy;

	@Data
	public static class ProxyInfo {

		private String type;

		private String host;

		private List<String> servers;

	}

	public void setProxyInfo(List<ProxyInfo> proxyInfos) {
		this.proxy = proxyInfos;
	}
}
