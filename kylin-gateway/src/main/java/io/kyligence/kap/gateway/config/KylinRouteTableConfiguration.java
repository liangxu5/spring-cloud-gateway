package io.kyligence.kap.gateway.config;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import io.kyligence.kap.gateway.constant.KylinGatewayVersion;
import io.kyligence.kap.gateway.entity.KylinJdbcDataSource;
import io.kyligence.kap.gateway.persistent.FileDataSource;
import io.kyligence.kap.gateway.route.reader.*;
import io.kyligence.kap.gateway.persistent.KylinJdbcTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration
@EnableConfigurationProperties
public class KylinRouteTableConfiguration {

	@Value(value = "${kylin.gateway.ke.version:4x}")
	private String version;

	@Bean
	@ConditionalOnProperty(name = "kylin.gateway.datasource.type", havingValue = "jdbc")
	@ConfigurationProperties(prefix = "kylin.gateway.datasource")
	public KylinJdbcDataSource kylinJdbcDataSource() {
		return new KylinJdbcDataSource();
	}

	@Bean
	@ConditionalOnProperty(name = "kylin.gateway.datasource.type", havingValue = "jdbc")
	public IRouteTableReader kylinRouteStore(KylinJdbcDataSource kylinJdbcDataSource) {
		DataSource dataSource = DataSourceBuilder.create().type(MysqlDataSource.class)
				.driverClassName(kylinJdbcDataSource.getDriverClassName())
				.url(kylinJdbcDataSource.getUrl())
				.username(kylinJdbcDataSource.getUsername())
				.password(kylinJdbcDataSource.getPassword()).build();

		if (KylinGatewayVersion.KYLIN_3X.equals(version)) {
			return new Kylin3XJdbcRouteTableReader(
					new KylinJdbcTemplate(dataSource, kylinJdbcDataSource.getTableName()),
					kylinJdbcDataSource.getTableName());
		}

		return new KylinJdbcRouteTableReader(new KylinJdbcTemplate(dataSource, kylinJdbcDataSource.getTableName()),
				kylinJdbcDataSource.getTableName(), kylinJdbcDataSource.getClusterId());
	}

	@Bean
	@ConditionalOnProperty(name = "kylin.gateway.datasource.type", havingValue = "file")
	@ConfigurationProperties(prefix = "kylin.gateway.datasource")
	public FileDataSource fileDataSource() {
		return new FileDataSource();
	}

	@Bean
	@ConditionalOnProperty(name = "kylin.gateway.datasource.type", havingValue = "file")
	public IRouteTableReader fileRouteTableReader(FileDataSource fileDataSource) {
		return new FileRouteTableReader(fileDataSource);
	}

	@Bean
	@ConditionalOnProperty(name = "kylin.gateway.datasource.type", havingValue = "mock")
	public IRouteTableReader mockRouteTableReader() {
		return new MockRouteTableReader();
	}

	@Bean
	@ConditionalOnProperty(name = "kylin.gateway.datasource.type", havingValue = "config")
	public IRouteTableReader configRouteTableReader() {
		return new ConfigRouteTableReader();
	}

}
