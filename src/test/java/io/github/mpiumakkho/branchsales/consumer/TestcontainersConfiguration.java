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

	/** Partitions of the dead-letter topic. */
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

	// In infra a branch topic is created by onboard-branch.sh (1 partition). The examples use BR0001, BR0002 and BR9999
	// (BR9999 has a topic but is not in the branch registry).
	@Bean
	NewTopic topicBr0001() {
		return new NewTopic(ContractExamples.topicOf("BR0001"), 1, (short) 1);
	}

	@Bean
	NewTopic topicBr0002() {
		return new NewTopic(ContractExamples.topicOf("BR0002"), 1, (short) 1);
	}

	@Bean
	NewTopic topicBr9999() {
		return new NewTopic(ContractExamples.topicOf("BR9999"), 1, (short) 1);
	}

	@Bean
	NewTopic deadLetterTopic(@Value("${branch-sales.kafka.dead-letter-topic}") String name) {
		return new NewTopic(name, PARTITIONS, (short) 1);
	}
}
