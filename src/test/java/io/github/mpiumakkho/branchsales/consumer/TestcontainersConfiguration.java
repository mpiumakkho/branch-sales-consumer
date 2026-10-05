package io.github.mpiumakkho.branchsales.consumer;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * HQ database and the Kafka brokers of two branches, with the same images as the HQ and branch compose files.
 * The branches are connected through the branch registry ({@link TestBranch#register}), as in production.
 */
@TestConfiguration(proxyBeanMethods = false)
public class TestcontainersConfiguration {

	@Bean
	@ServiceConnection
	PostgreSQLContainer postgresContainer() {
		return new PostgreSQLContainer(DockerImageName.parse("postgres:18.6-alpine"));
	}

	@Bean
	KafkaContainer br0001Kafka() {
		return branchKafka();
	}

	@Bean
	KafkaContainer br0002Kafka() {
		return branchKafka();
	}

	private static KafkaContainer branchKafka() {
		return new KafkaContainer(DockerImageName.parse("apache/kafka:4.3.1"));
	}
}
