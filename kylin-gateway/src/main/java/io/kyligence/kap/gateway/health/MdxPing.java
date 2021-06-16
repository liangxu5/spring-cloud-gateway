package io.kyligence.kap.gateway.health;

import com.alibaba.fastjson.JSONObject;
import com.netflix.loadbalancer.IPing;
import com.netflix.loadbalancer.Server;
import io.kyligence.kap.gateway.constant.KylinGatewayVersion;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import java.util.Objects;

@Component
@Slf4j
@ConditionalOnProperty(name = "kylin.gateway.ke.version", havingValue = KylinGatewayVersion.MDX)
public class MdxPing implements IPing {
	private static final String HEALTH_URL_FROMAT = "http://%s%s";

	@Autowired
	private RestTemplate restTemplate;

	@Value("${kylin.gateway.health.check-url:/api/system/health}")
	private String healthUrl;

	@Value("${kylin.gateway.health.load_url:/api/system/load}")
	private String loadUrl;

	@Override
	public boolean isAlive(Server server) {
		return ErrorLevel.NORMAL == checkServer(server);
	}

	public enum ErrorLevel {
		NORMAL,
		WARN,
		ERROR,
		FATAL,
		;
	}

	public ErrorLevel checkServer(Server server) {
		if (Objects.isNull(server)) {
			return ErrorLevel.FATAL;
		}

		String healthCheckUrl = String.format(HEALTH_URL_FROMAT, server.getId(), healthUrl);

		try {
			ResponseEntity<String> responseEntity = restTemplate.getForEntity(healthCheckUrl, String.class);
			if (responseEntity.getStatusCode().is2xxSuccessful()) {
				return ErrorLevel.NORMAL;
			}
		} catch (Exception e) {
			log.warn("health check failed, server: {}", server);
			log.debug("health check failed!", e);

			if (e instanceof ResourceAccessException) {
				return ErrorLevel.FATAL;
			}

			return ErrorLevel.ERROR;
		}

		return ErrorLevel.WARN;
	}

	public Double getServerLoad(Server server) {

		if (Objects.isNull(server)) {
			return Double.MAX_VALUE;
		}

		String heathLoadUrl = String.format(HEALTH_URL_FROMAT, server.getId(), loadUrl);

		try {
			ResponseEntity<String> responseEntity = restTemplate.getForEntity(heathLoadUrl, String.class);
			if (responseEntity.getStatusCode().is2xxSuccessful()) {
				String result = responseEntity.getBody();
				JSONObject jsonObject = JSONObject.parseObject(result);
				String load = jsonObject.getString("msg");
				return Double.valueOf(load);
			}
		} catch (Exception e) {
			// Nothing to do
		}
		return Double.MAX_VALUE;
	}


}
