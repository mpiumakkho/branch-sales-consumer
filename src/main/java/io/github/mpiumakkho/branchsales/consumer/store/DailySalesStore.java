package io.github.mpiumakkho.branchsales.consumer.store;

import java.util.List;
import java.util.Optional;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import io.github.mpiumakkho.branchsales.consumer.summary.DailySalesSummary;

/**
 * Stores a summary by revision (rules R4–R6). Must run inside a transaction so
 * the header and its lines change together.
 */
@Repository
public class DailySalesStore {

	public enum Outcome {
		/** No row for (branchCode, saleDate) yet. */
		INSERTED,
		/** Higher revision replaced the stored header and lines. */
		UPDATED,
		/** Same revision as stored: a repeated delivery (R5). */
		DUPLICATE,
		/** Lower revision than stored: an old message arriving late (R6). */
		STALE
	}

	public record Result(Outcome outcome, int storedRevision) {
	}

	// The WHERE on DO UPDATE makes the revision check and the write one atomic statement, so two consumers
	// handling the same key during a rebalance cannot overwrite a higher revision.
	// RETURNING old.* needs PostgreSQL 18; old.revision is null when the row was inserted.
	private static final String UPSERT_HEADER = """
			insert into branch_daily_sales (branch_code, sale_date, revision, total_amount, confirmed_at, event_id)
			values (:branchCode, :saleDate, :revision, :totalAmount, :confirmedAt, :eventId)
			on conflict (branch_code, sale_date) do update
			   set revision     = excluded.revision,
			       total_amount = excluded.total_amount,
			       confirmed_at = excluded.confirmed_at,
			       event_id     = excluded.event_id,
			       received_at  = now()
			 where branch_daily_sales.revision < excluded.revision
			returning new.id, old.revision as previous_revision
			""";

	private static final String INSERT_LINE = """
			insert into branch_daily_sales_line (branch_daily_sales_id, category_code, amount, quantity)
			values (?, ?, ?, ?)
			""";

	private final JdbcClient jdbc;
	private final JdbcTemplate jdbcTemplate;

	public DailySalesStore(JdbcClient jdbc, JdbcTemplate jdbcTemplate) {
		this.jdbc = jdbc;
		this.jdbcTemplate = jdbcTemplate;
	}

	public Result apply(DailySalesSummary summary) {
		Optional<Written> written = jdbc.sql(UPSERT_HEADER)
				.param("branchCode", summary.branchCode())
				.param("saleDate", summary.saleDate())
				.param("revision", summary.revision())
				.param("totalAmount", summary.totalAmount())
				.param("confirmedAt", summary.confirmedAt())
				.param("eventId", summary.eventId())
				.query((rs, n) -> new Written(rs.getLong("id"), rs.getObject("previous_revision", Integer.class)))
				.optional();

		if (written.isEmpty()) {
			int stored = storedRevision(summary);
			return new Result(stored == summary.revision() ? Outcome.DUPLICATE : Outcome.STALE, stored);
		}

		long headerId = written.get().id();
		jdbc.sql("delete from branch_daily_sales_line where branch_daily_sales_id = :id")
				.param("id", headerId)
				.update();
		List<Object[]> lines = summary.lines().stream()
				.map(l -> new Object[] { headerId, l.categoryCode(), l.amount(), l.quantity() })
				.toList();
		jdbcTemplate.batchUpdate(INSERT_LINE, lines);

		Outcome outcome = written.get().previousRevision() == null ? Outcome.INSERTED : Outcome.UPDATED;
		return new Result(outcome, summary.revision());
	}

	private int storedRevision(DailySalesSummary summary) {
		return jdbc.sql("select revision from branch_daily_sales where branch_code = :branchCode and sale_date = :saleDate")
				.param("branchCode", summary.branchCode())
				.param("saleDate", summary.saleDate())
				.query(Integer.class)
				.single();
	}

	private record Written(long id, Integer previousRevision) {
	}
}
