package io.github.mpiumakkho.branchsales.consumer.exception;

/**
 * Why a record was rejected and stored in the HQ {@code dead_letter} table. Values and order follow the
 * validation layers in {@code contract/README.md}.
 */
public enum RejectReason {
	INVALID_JSON,
	SCHEMA_INVALID,
	BRANCH_MISMATCH,
	KEY_MISMATCH,
	TOTAL_MISMATCH,
	DUPLICATE_CATEGORY,
	UNKNOWN_BRANCH,
	UNKNOWN_CATEGORY
}
