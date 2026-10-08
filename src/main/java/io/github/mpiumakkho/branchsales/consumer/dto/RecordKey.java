package io.github.mpiumakkho.branchsales.consumer.dto;

import java.time.LocalDate;

/**
 * The key of a record within its branch and type ({@link RecordType.KeyShape}): HQ keeps one row per (branchCode,
 * key) holding the highest revision.
 */
public sealed interface RecordKey permits RecordKey.DayKey, RecordKey.ShiftKey {

	/** The business date of the record. */
	LocalDate date();

	/** The key for logs and messages, e.g. {@code 2026-10-01} or {@code 2026-10-01 POS01 shift 1}. */
	String text();

	/** A daily record: the business date only. */
	record DayKey(LocalDate date) implements RecordKey {

		@Override
		public String text() {
			return date.toString();
		}
	}

	/** One shift of one POS terminal on a business date. */
	record ShiftKey(LocalDate date, String terminalId, int shiftNo) implements RecordKey {

		@Override
		public String text() {
			return date + " " + terminalId + " shift " + shiftNo;
		}
	}
}
