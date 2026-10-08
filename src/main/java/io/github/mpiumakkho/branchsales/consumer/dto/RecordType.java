package io.github.mpiumakkho.branchsales.consumer.dto;

import io.github.mpiumakkho.branchsales.consumer.exception.RejectReason;

/**
 * The kinds of record a branch sends, one topic each (contract/README.md). Every type has the same outline: a key,
 * a revision, a total and lines of (code, amount, quantity). What differs is the topic, the schema, the key fields
 * ({@link KeyShape}), the code dimension of the lines ({@link LineShape}), the HQ tables, and the reference checks.
 */
public enum RecordType {

	/** Daily sales (contract {@code DailySalesSummary}). */
	DAILY_SUMMARY("/contract/daily-sales-summary.v1.schema.json", new KeyShape(KeyShape.Kind.DAY, "saleDate", "sale_date"),
			LineShape.CATEGORY, "branch_daily_sales", "branch_daily_sales_line", "branch_daily_sales_id"),

	/** Daily returns and voids (contract {@code DailyReturn}); needs the daily sales of the same date at HQ. */
	DAILY_RETURN("/contract/daily-return.v1.schema.json", new KeyShape(KeyShape.Kind.DAY, "returnDate", "return_date"),
			LineShape.CATEGORY, "branch_daily_return", "branch_daily_return_line", "branch_daily_return_id"),

	/**
	 * Shift close of one POS terminal (contract {@code ShiftClose}), by tender; stored with or without the daily
	 * sales of its business date.
	 */
	SHIFT_CLOSE("/contract/shift-close.v1.schema.json", new KeyShape(KeyShape.Kind.SHIFT, "businessDate", "business_date"),
			LineShape.TENDER, "branch_shift_close", "branch_shift_close_tender", "branch_shift_close_id");

	/**
	 * The fields that identify one record of a branch, besides branchCode.
	 *
	 * @param kind       DAY: the date only. SHIFT: the date, {@code terminalId}/{@code terminal_id} and
	 *                   {@code shiftNo}/{@code shift_no} (fixed in code, not data)
	 * @param dateField  name of the business-date field in the message
	 * @param dateColumn column of the business date in the HQ header table
	 */
	public record KeyShape(Kind kind, String dateField, String dateColumn) {

		public enum Kind {
			DAY, SHIFT
		}
	}

	/**
	 * The code dimension of the lines: the message field, the HQ column, the HQ master table the codes must exist in,
	 * and the reasons used when a code is repeated or unknown.
	 */
	public record LineShape(String codeField, String codeColumn, String referenceTable, RejectReason duplicateReason,
			RejectReason unknownReason) {

		/** Lines by product category (contract/categories.md). */
		public static final LineShape CATEGORY = new LineShape("categoryCode", "category_code", "category",
				RejectReason.DUPLICATE_CATEGORY, RejectReason.UNKNOWN_CATEGORY);

		/** Lines by tender type (contract/tender-types.md). */
		public static final LineShape TENDER = new LineShape("tenderType", "tender_type", "tender_type",
				RejectReason.DUPLICATE_TENDER, RejectReason.UNKNOWN_TENDER);
	}

	private final String schemaResource;
	private final KeyShape keyShape;
	private final LineShape lineShape;
	private final String table;
	private final String lineTable;
	private final String lineForeignKey;

	RecordType(String schemaResource, KeyShape keyShape, LineShape lineShape, String table, String lineTable,
			String lineForeignKey) {
		this.schemaResource = schemaResource;
		this.keyShape = keyShape;
		this.lineShape = lineShape;
		this.table = table;
		this.lineTable = lineTable;
		this.lineForeignKey = lineForeignKey;
	}

	/** Classpath resource of the JSON Schema (copied from contract/ at build time). */
	public String schemaResource() {
		return schemaResource;
	}

	/** The key fields of a record of this type. */
	public KeyShape keyShape() {
		return keyShape;
	}

	/** The code dimension of the lines. */
	public LineShape lineShape() {
		return lineShape;
	}

	/** Name of the business-date field in the message. */
	public String dateField() {
		return keyShape.dateField();
	}

	/** HQ header table: one row per (branch_code, key) holding the highest revision. */
	public String table() {
		return table;
	}

	/** HQ line table of the header table. */
	public String lineTable() {
		return lineTable;
	}

	/** Column of the line table that references the header row. */
	public String lineForeignKey() {
		return lineForeignKey;
	}
}
