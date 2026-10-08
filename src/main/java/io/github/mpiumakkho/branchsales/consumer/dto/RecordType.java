package io.github.mpiumakkho.branchsales.consumer.dto;

/**
 * The kinds of record a branch sends, one topic each (contract/README.md). Both have the same shape: one business
 * day of one branch, broken down by category, with a revision. What differs is the topic, the schema, the name of
 * the date field, the HQ tables, and the reference checks.
 */
public enum RecordType {

	/** Daily sales (contract {@code DailySalesSummary}). */
	DAILY_SUMMARY("/contract/daily-sales-summary.v1.schema.json", "saleDate", "branch_daily_sales",
			"branch_daily_sales_line", "branch_daily_sales_id"),

	/** Daily returns and voids (contract {@code DailyReturn}); needs the daily sales of the same date at HQ. */
	DAILY_RETURN("/contract/daily-return.v1.schema.json", "returnDate", "branch_daily_return",
			"branch_daily_return_line", "branch_daily_return_id");

	private final String schemaResource;
	private final String dateField;
	private final String table;
	private final String lineTable;
	private final String lineForeignKey;

	RecordType(String schemaResource, String dateField, String table, String lineTable, String lineForeignKey) {
		this.schemaResource = schemaResource;
		this.dateField = dateField;
		this.table = table;
		this.lineTable = lineTable;
		this.lineForeignKey = lineForeignKey;
	}

	/** Classpath resource of the JSON Schema (copied from contract/ at build time). */
	public String schemaResource() {
		return schemaResource;
	}

	/** Name of the business-date field in the message. */
	public String dateField() {
		return dateField;
	}

	/** HQ header table: one row per (branch_code, date) holding the highest revision. */
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
