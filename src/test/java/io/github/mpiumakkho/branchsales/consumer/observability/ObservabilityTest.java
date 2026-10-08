package io.github.mpiumakkho.branchsales.consumer.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.web.client.RestClient;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import io.github.mpiumakkho.branchsales.consumer.ContractExamples;
import io.github.mpiumakkho.branchsales.consumer.TestBranch;
import io.github.mpiumakkho.branchsales.consumer.TestcontainersConfiguration;
import io.github.mpiumakkho.branchsales.consumer.listener.BranchListeners;

/**
 * The health endpoint follows the branch connections and the Prometheus endpoint carries the record counter and the
 * branch gauges. Served on a random port here; CONSUMER_HTTP_PORT in production.
 */
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
@Import(TestcontainersConfiguration.class)
class ObservabilityTest {

	private static final Duration TIMEOUT = Duration.ofSeconds(30);
	private static final JsonMapper MAPPER = JsonMapper.builder().build();

	@Autowired
	JdbcClient jdbc;

	@Autowired
	BranchListeners listeners;

	@Autowired
	Environment environment;

	@Autowired
	@Qualifier("br0001Kafka")
	KafkaContainer br0001Kafka;

	private TestBranch br0001;
	private RestClient http;

	@BeforeEach
	void setUp() {
		jdbc.sql("delete from branch_daily_sales").update();
		jdbc.sql("delete from dead_letter").update();
		http = RestClient.create("http://localhost:" + environment.getRequiredProperty("local.server.port"));
		br0001 = TestBranch.open("BR0001", br0001Kafka);
		br0001.register(jdbc);
		// A branch whose host name does not resolve: registered but never connected
		jdbc.sql("""
				insert into branch (branch_code, name, kafka_bootstrap) values ('BR0003', 'Unreachable', 'kafka.br0003.invalid:9094')
				on conflict (branch_code) do update set kafka_bootstrap = excluded.kafka_bootstrap
				""").update();
		await().atMost(TIMEOUT).until(() -> listeners.connectedBranches().contains("BR0001")
				&& listeners.unreachableBranches().containsKey("BR0003"));
	}

	@AfterEach
	void tearDown() {
		br0001.close();
		jdbc.sql("update branch set kafka_bootstrap = null where branch_code in ('BR0001', 'BR0003')").update();
		await().atMost(TIMEOUT).until(() -> listeners.connectedBranches().isEmpty());
	}

	@Test
	void healthReportsConnectionsAndIsDownWithoutAny() {
		JsonNode branches = health().path("components").path("branches");
		assertThat(health().path("status").asString()).isEqualTo("UP");
		assertThat(branches.path("status").asString()).isEqualTo("UP");
		assertThat(branches.path("details").path("registered").asInt()).isEqualTo(2);
		assertThat(branches.path("details").path("connected").asInt()).isEqualTo(1);
		assertThat(branches.path("details").path("unreachable").path("BR0003").asString())
				.startsWith("kafka.br0003.invalid:9094: ");
		assertThat(branches.path("details").path("shard").asString()).isEqualTo("default");

		// Only the unreachable branch left: nothing is read, the instance is DOWN
		jdbc.sql("update branch set kafka_bootstrap = null where branch_code = 'BR0001'").update();
		await().atMost(TIMEOUT).until(() -> listeners.connectedBranches().isEmpty());
		await().atMost(TIMEOUT).untilAsserted(() -> {
			JsonNode down = health();
			assertThat(down.path("status").asString()).isEqualTo("DOWN");
			assertThat(down.path("components").path("branches").path("details").path("registered").asInt()).isEqualTo(1);
		});
	}

	@Test
	void prometheusCountsRecordsAndBranches() {
		br0001.send(ContractExamples.read("valid/basic.json"));
		br0001.readReceipts(1);

		await().atMost(TIMEOUT).untilAsserted(() -> {
			List<String> lines = prometheus();
			assertThat(lines).contains(
					"branch_sales_receipts_total{outcome=\"INSERTED\",reason=\"none\",type=\"DAILY_SUMMARY\"} 1.0",
					"branch_sales_receipts_total{outcome=\"REJECTED\",reason=\"PARENT_MISSING\",type=\"DAILY_RETURN\"} 0.0",
					"branch_sales_branches_connected 1.0",
					"branch_sales_branches_unreachable 1.0",
					"branch_sales_branches_registered 2.0",
					"branch_sales_dead_letters_open 0.0");
		});
	}

	private JsonNode health() {
		String body = http.get().uri("/actuator/health").retrieve()
				.onStatus(status -> status.value() == 503, (request, response) -> { }) // DOWN is 503, still a body
				.body(String.class);
		return MAPPER.readTree(body);
	}

	private List<String> prometheus() {
		return http.get().uri("/actuator/prometheus").retrieve().body(String.class).lines().toList();
	}
}
