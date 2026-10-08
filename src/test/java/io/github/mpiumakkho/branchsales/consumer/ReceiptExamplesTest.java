package io.github.mpiumakkho.branchsales.consumer;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

/** Every file in contract/receipt-examples matches the receipt schema. */
class ReceiptExamplesTest {

	private static final Path ROOT = Path.of("contract", "receipt-examples");

	static final List<String> EXAMPLES = list();

	@ParameterizedTest
	@FieldSource("EXAMPLES")
	void receiptExampleMatchesTheSchema(String example) throws IOException {
		assertThat(ReceiptSchema.validate(Files.readAllBytes(ROOT.resolve(example))).get("outcome")).isNotNull();
	}

	private static List<String> list() {
		try (Stream<Path> files = Files.list(ROOT)) {
			return files.map(p -> p.getFileName().toString()).filter(n -> n.endsWith(".json")).sorted().toList();
		}
		catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}
}
