package io.github.mpiumakkho.branchsales.consumer.service;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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

import io.github.mpiumakkho.branchsales.consumer.dto.DailyFigures;
import io.github.mpiumakkho.branchsales.consumer.dto.DailyFigures.Line;
import io.github.mpiumakkho.branchsales.consumer.dto.RecordType;
import io.github.mpiumakkho.branchsales.consumer.exception.RejectReason;
import io.github.mpiumakkho.branchsales.consumer.exception.RejectedMessageException;

/**
 * Parse, schema, identity and business layers of the contract checks, for every record type (one JSON Schema
 * each). Needs no database, so the reference layer is done separately by {@link ReferenceChecker}.
 */
@Component
public class RecordValidator {

	private static final int MAX_SCHEMA_ERRORS_IN_DETAIL = 5;

	private final JsonMapper mapper = JsonMapper.builder()
			// A repeated key would make it unclear which value the branch meant
			.enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
			.enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
			.build();

	private final Map<RecordType, Schema> schemas = new EnumMap<>(RecordType.class);

	public RecordValidator() {
		// Format assertions are off by default since draft 2019-09; the contract requires uuid, date and date-time checks.
		var config = SchemaRegistryConfig.builder()
				.formatAssertionsEnabled(true)
				.locale(Locale.ENGLISH)
				.build();
		var registry = SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12,
				builder -> builder.schemaRegistryConfig(config));
		for (RecordType type : RecordType.values()) {
			try (InputStream in = RecordValidator.class.getResourceAsStream(type.schemaResource())) {
				if (in == null) {
					throw new IllegalStateException("JSON Schema not found on classpath: " + type.schemaResource());
				}
				schemas.put(type, registry.getSchema(in));
			}
			catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		}
	}

	/**
	 * @param type       the record type of the topic the record was read from
	 * @param branchCode the branch whose Kafka cluster the record was read from (branch registry)
	 * @param key        the record key
	 * @throws RejectedMessageException if the record fails a parse, schema, identity or business check
	 */
	public DailyFigures validate(RecordType type, String branchCode, String key, byte[] value) {
		JsonNode root = parse(value);
		checkSchema(type, root);
		DailyFigures figures = toFigures(type, root);
		checkIdentity(figures, branchCode, key);
		checkTotal(figures);
		checkUniqueCategories(figures);
		return figures;
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

	private void checkSchema(RecordType type, JsonNode root) {
		List<Error> errors = schemas.get(type).validate(root);
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
	private DailyFigures toFigures(RecordType type, JsonNode root) {
		try {
			List<Line> lines = new ArrayList<>();
			for (JsonNode line : root.get("lines")) {
				lines.add(new Line(
						line.get("categoryCode").asString(),
						new BigDecimal(line.get("amount").asString()),
						exactLong(line.get("quantity"), "quantity")));
			}
			return new DailyFigures(
					type,
					UUID.fromString(root.get("eventId").asString()),
					root.get("branchCode").asString(),
					LocalDate.parse(root.get(type.dateField()).asString()),
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
	 * HQ reaches each branch's Kafka through the address registered for that branch, so the cluster identifies the
	 * sender (requirements Q5, §11). The key must still be the branch code (Q7).
	 */
	private static void checkIdentity(DailyFigures figures, String branchCode, String key) {
		if (!branchCode.equals(figures.branchCode())) {
			throw new RejectedMessageException(RejectReason.BRANCH_MISMATCH,
					"branchCode " + figures.branchCode() + " read from the Kafka of branch " + branchCode, figures);
		}
		if (!figures.branchCode().equals(key)) {
			throw new RejectedMessageException(RejectReason.KEY_MISMATCH,
					"record key " + (key == null ? "missing" : "'" + key + "'") + ", branchCode " + figures.branchCode(),
					figures);
		}
	}

	private static void checkTotal(DailyFigures figures) {
		BigDecimal sum = figures.lines().stream()
				.map(Line::amount)
				.reduce(BigDecimal.ZERO, BigDecimal::add);
		if (figures.totalAmount().compareTo(sum) != 0) {
			throw new RejectedMessageException(RejectReason.TOTAL_MISMATCH,
					"totalAmount " + figures.totalAmount() + " but lines sum to " + sum, figures);
		}
	}

	private static void checkUniqueCategories(DailyFigures figures) {
		Set<String> seen = new HashSet<>();
		Set<String> duplicates = new TreeSet<>();
		for (Line line : figures.lines()) {
			if (!seen.add(line.categoryCode())) {
				duplicates.add(line.categoryCode());
			}
		}
		if (!duplicates.isEmpty()) {
			throw new RejectedMessageException(RejectReason.DUPLICATE_CATEGORY, "repeated categoryCode " + duplicates,
					figures);
		}
	}
}
