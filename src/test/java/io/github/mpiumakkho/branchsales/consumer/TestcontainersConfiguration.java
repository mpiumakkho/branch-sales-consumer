package io.github.mpiumakkho.branchsales.consumer;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/** Same images as infra/docker-compose.yml. */
@TestConfiguration(proxyBeanMethods = false)
public class TestcontainersConfiguration {

	public static final int PARTITIONS = 2;

	@Bean
	@ServiceConnection
	KafkaContainer kafkaContainer() {
		return new KafkaContainer(DockerImageName.parse("apache/kafka:4.3.1"));
	}

	@Bean
	@ServiceConnection
	PostgreSQLContainer postgresContainer() {
		return new PostgreSQLContainer(DockerImageName.parse("postgres:18.6-alpine"));
	}

	// In infra the topics are created by kafka-init. Both need the same partition count,
	// because a dead letter goes to the same partition number as its source record.
	@Bean
	NewTopic summaryTopic(@Value("${branch-sales.kafka.topic}") String name) {
		return new NewTopic(name, PARTITIONS, (short) 1);
	}

	@Bean
	NewTopic deadLetterTopic(@Value("${branch-sales.kafka.dead-letter-topic}") String name) {
		return new NewTopic(name, PARTITIONS, (short) 1);
	}
}
