package io.github.mpiumakkho.branchsales.consumer.service;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;

import com.networknt.schema.Error;
import com.networknt.schema.Schema;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SchemaRegistryConfig;
import com.networknt.schema.SpecificationVersion;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.core.StreamReadFeature;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import io.github.mpiumakkho.branchsales.consumer.dto.DailySalesSummary;
import io.github.mpiumakkho.branchsales.consumer.dto.DailySalesSummary.SalesLine;
import io.github.mpiumakkho.branchsales.consumer.exception.RejectReason;
import io.github.mpiumakkho.branchsales.consumer.exception.RejectedMessageException;

/**
 * Parse, schema, identity and business layers of the contract checks. Needs no
 * database, so the reference layer is done separately by {@link ReferenceChecker}.
 */
@Component
public class SummaryValidator {

	static final String SCHEMA_RESOURCE = "/contract/daily-sales-summary.v1.schema.json";

	private static final int MAX_SCHEMA_ERRORS_IN_DETAIL = 5;

	private final JsonMapper mapper = JsonMapper.builder()
			// A repeated key would make it unclear which value the branch meant
			.enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
			.enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
			.build();

	private final Schema schema;

	public SummaryValidator() {
		// Format assertions are off by default since draft 2019-09; the contract requires uuid, date and date-time checks.
		var config = SchemaRegistryConfig.builder()
				.formatAssertionsEnabled(true)
				.locale(Locale.ENGLISH)
				.build();
		var registry = SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12,
				builder -> builder.schemaRegistryConfig(config));
		try (InputStream in = SummaryValidator.class.getResourceAsStream(SCHEMA_RESOURCE)) {
			if (in == null) {
				throw new IllegalStateException("JSON Schema not found on classpath: " + SCHEMA_RESOURCE);
			}
			this.schema = registry.getSchema(in);
		}
		catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	/**
	 * @param topic the topic the record was read from: {@code branch-sales.daily-summary.<branchCode>}
	 * @param key   the record key
	 * @throws RejectedMessageException if the record fails a parse, schema, identity or business check
	 */
	public DailySalesSummary validate(String topic, String key, byte[] value) {
		JsonNode root = parse(value);
		checkSchema(root);
		DailySalesSummary summary = toSummary(root);
		checkIdentity(summary, topic, key);
		checkTotal(summary);
		checkUniqueCategories(summary);
		return summary;
	}

	private JsonNode parse(byte[] value) {
		if (value == null || value.length == 0) {
			throw new RejectedMessageException(RejectReason.INVALID_JSON, "empty value");
		}
		try {
			return mapper.readTree(value);
		}
		catch (JacksonException e) {
			throw new RejectedMessageException(RejectReason.INVALID_JSON, e.getOriginalMessage());
		}
	}

	private void checkSchema(JsonNode root) {
		List<Error> errors = schema.validate(root);
		if (!errors.isEmpty()) {
			String detail = errors.stream()
					.limit(MAX_SCHEMA_ERRORS_IN_DETAIL)
					.map(Error::toString)
					.collect(Collectors.joining("; "));
			if (errors.size() > MAX_SCHEMA_ERRORS_IN_DETAIL) {
				detail += "; and " + (errors.size() - MAX_SCHEMA_ERRORS_IN_DETAIL) + " more";
			}
			throw new RejectedMessageException(RejectReason.SCHEMA_INVALID, detail);
		}
	}

	// Called only after the schema check, so required fields are present and have the right JSON types.
	private DailySalesSummary toSummary(JsonNode root) {
		try {
			List<SalesLine> lines = new ArrayList<>();
			for (JsonNode line : root.get("lines")) {
				lines.add(new SalesLine(
						line.get("categoryCode").asString(),
						new BigDecimal(line.get("amount").asString()),
						exactLong(line.get("quantity"), "quantity")));
			}
			return new DailySalesSummary(
					UUID.fromString(root.get("eventId").asString()),
					root.get("branchCode").asString(),
					LocalDate.parse(root.get("saleDate").asString()),
					exactInt(root.get("revision"), "revision"),
					OffsetDateTime.parse(root.get("confirmedAt").asString()),
					new BigDecimal(root.get("totalAmount").asString()),
					lines);
		}
		catch (DateTimeParseException | IllegalArgumentException | ArithmeticException e) {
			// Valid by the schema but not representable in HQ types (e.g. a revision above Integer.MAX_VALUE)
			throw new RejectedMessageException(RejectReason.SCHEMA_INVALID, "value cannot be stored: " + e.getMessage());
		}
	}

	private static int exactInt(JsonNode node, String field) {
		if (!node.canConvertToInt()) {
			throw new ArithmeticException(field + " out of range: " + node);
		}
		return node.intValue();
	}

	private static long exactLong(JsonNode node, String field) {
		if (!node.canConvertToLong()) {
			throw new ArithmeticException(field + " out of range: " + node);
		}
		return node.longValue();
	}

	/**
	 * Only the branch's own SCRAM user may write its topic (ACL set by infra/onboard-branch.sh), so the topic suffix
	 * identifies the sender (requirements Q5). The key decides the partition and so the order of a branch's records (Q7).
	 */
	private static void checkIdentity(DailySalesSummary summary, String topic, String key) {
		// The listener's topic pattern guarantees the suffix after the last dot is a branch code
		String topicBranch = topic.substring(topic.lastIndexOf('.') + 1);
		if (!topicBranch.equals(summary.branchCode())) {
			throw new RejectedMessageException(RejectReason.BRANCH_MISMATCH,
					"branchCode " + summary.branchCode() + " sent on the topic of branch " + topicBranch);
		}
		if (!summary.branchCode().equals(key)) {
			throw new RejectedMessageException(RejectReason.KEY_MISMATCH,
					"record key " + (key == null ? "missing" : "'" + key + "'") + ", branchCode " + summary.branchCode());
		}
	}

	private static void checkTotal(DailySalesSummary summary) {
		BigDecimal sum = summary.lines().stream()
				.map(SalesLine::amount)
				.reduce(BigDecimal.ZERO, BigDecimal::add);
		if (summary.totalAmount().compareTo(sum) != 0) {
			throw new RejectedMessageException(RejectReason.TOTAL_MISMATCH,
					"totalAmount " + summary.totalAmount() + " but lines sum to " + sum);
		}
	}

	private static void checkUniqueCategories(DailySalesSummary summary) {
		Set<String> seen = new HashSet<>();
		Set<String> duplicates = new TreeSet<>();
		for (SalesLine line : summary.lines()) {
			if (!seen.add(line.categoryCode())) {
				duplicates.add(line.categoryCode());
			}
		}
		if (!duplicates.isEmpty()) {
			throw new RejectedMessageException(RejectReason.DUPLICATE_CATEGORY, "repeated categoryCode " + duplicates);
		}
	}
}
