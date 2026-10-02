package io.github.mpiumakkho.branchsales.consumer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/** Reads the example messages in contract/examples (tests run with the project root as working directory). */
public final class ContractExamples {

	private static final Path ROOT = Path.of("contract", "examples");
	private static final Pattern BRANCH_CODE = Pattern.compile("\"branchCode\"\\s*:\\s*\"([^\"]*)\"");

	/** Topic of a branch, as created by infra/onboard-branch.sh. */
	public static String topicOf(String branchCode) {
		return "branch-sales.daily-summary." + branchCode;
	}

	/** The branchCode in a message value, or BR0001 if there is none (e.g. not JSON): the branch a producer would send it as. */
	public static String branchCodeOf(byte[] value) {
		Matcher m = BRANCH_CODE.matcher(new String(value == null ? new byte[0] : value, StandardCharsets.UTF_8));
		return m.find() ? m.group(1) : "BR0001";
	}

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
