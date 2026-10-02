package io.github.mpiumakkho.branchsales.consumer.validation;

/**
 * Why a record was sent to the dead-letter topic. Values and order follow the
 * validation layers in {@code contract/README.md}.
 */
public enum RejectReason {
	INVALID_JSON,
	SCHEMA_INVALID,
	TOTAL_MISMATCH,
	DUPLICATE_CATEGORY,
	UNKNOWN_BRANCH,
	UNKNOWN_CATEGORY
}
