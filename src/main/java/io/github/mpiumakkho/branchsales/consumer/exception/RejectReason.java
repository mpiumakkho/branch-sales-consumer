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
	UNKNOWN_CATEGORY,
	/** A return whose daily sales of the same date are not at HQ yet. Replayed automatically when they arrive. */
	PARENT_MISSING,
	DUPLICATE_TENDER,
	UNKNOWN_TENDER,
	/** A shift close whose closedAt is before its openedAt. */
	SHIFT_TIMES_INVALID
}
