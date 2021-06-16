package io.kyligence.kap.gateway.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.Map;

@Configuration
@ConfigurationProperties(prefix = "mdx")
@Data
public class MdxConfig {

	private Long cacheTime;

	List<ProxyInfo> proxy;

	@Data
	public static class ProxyInfo {

		private String type;

		private String host;

		private Map<String, String> config;

		private List<String> servers;

	}

	// TODO 用于配置更改
	public void setProxyInfo(List<ProxyInfo> proxyInfos) {
		this.proxy = proxyInfos;
	}
}
