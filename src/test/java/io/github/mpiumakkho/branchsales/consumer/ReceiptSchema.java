package io.github.mpiumakkho.branchsales.consumer;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

import com.networknt.schema.Schema;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SchemaRegistryConfig;
import com.networknt.schema.SpecificationVersion;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** contract/daily-sales-receipt.v1.schema.json, with format assertions, as the branch side would check it. */
public final class ReceiptSchema {

	private static final JsonMapper MAPPER = JsonMapper.builder().build();
	private static final Schema SCHEMA = load();

	private ReceiptSchema() {
	}

	/** @return the parsed receipt; fails the test if it does not match the schema */
	public static JsonNode validate(byte[] value) {
		JsonNode receipt = MAPPER.readTree(value);
		assertThat(SCHEMA.validate(receipt)).as("schema errors in receipt %s", receipt).isEmpty();
		return receipt;
	}

	private static Schema load() {
		var config = SchemaRegistryConfig.builder().formatAssertionsEnabled(true).locale(Locale.ENGLISH).build();
		var registry = SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12,
				builder -> builder.schemaRegistryConfig(config));
		try {
			return registry.getSchema(Files.readString(Path.of("contract", "daily-sales-receipt.v1.schema.json")));
		}
		catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}
}
