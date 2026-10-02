package io.github.mpiumakkho.branchsales.consumer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

/** Reads the example messages in contract/examples (tests run with the project root as working directory). */
public final class ContractExamples {

	private static final Path ROOT = Path.of("contract", "examples");

	private ContractExamples() {
	}

	/** @param name path under contract/examples, e.g. {@code valid/basic.json} */
	public static byte[] read(String name) {
		try {
			return Files.readAllBytes(ROOT.resolve(name));
		}
		catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	/** Every example file, as paths relative to contract/examples with forward slashes. */
	public static List<String> all() {
		try (Stream<Path> files = Files.walk(ROOT)) {
			return files.filter(p -> p.toString().endsWith(".json"))
					.map(p -> ROOT.relativize(p).toString().replace('\\', '/'))
					.sorted()
					.toList();
		}
		catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}
}
