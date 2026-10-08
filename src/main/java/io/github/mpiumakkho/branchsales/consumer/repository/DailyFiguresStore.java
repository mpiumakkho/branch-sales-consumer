package io.github.mpiumakkho.branchsales.consumer.repository;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import io.github.mpiumakkho.branchsales.consumer.dto.DailyFigures;
import io.github.mpiumakkho.branchsales.consumer.dto.RecordType;

/**
 * Stores a record by revision (rules R4–R6) in the tables of its type. Must run inside a transaction so the header
 * and its lines change together.
 */
@Repository
public class DailyFiguresStore {

	public enum Outcome {
		/** No row for (type, branchCode, date) yet. */
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

	private record Sql(String upsertHeader, String insertLine, String deleteLines, String storedRevision) {

		// The WHERE on DO UPDATE makes the revision check and the write one atomic statement, so two consumers
		// handling the same key during a rebalance cannot overwrite a higher revision.
		// RETURNING old.* needs PostgreSQL 18; old.revision is null when the row was inserted.
		// Table and column names come from the RecordType enum, not from input.
		static Sql of(RecordType type) {
			String date = type.dateField().equals("saleDate") ? "sale_date" : "return_date";
			return new Sql(
					"""
					insert into %1$s (branch_code, %2$s, revision, total_amount, confirmed_at, event_id)
					values (:branchCode, :date, :revision, :totalAmount, :confirmedAt, :eventId)
					on conflict (branch_code, %2$s) do update
					   set revision     = excluded.revision,
					       total_amount = excluded.total_amount,
					       confirmed_at = excluded.confirmed_at,
					       event_id     = excluded.event_id,
					       received_at  = now()
					 where %1$s.revision < excluded.revision
					returning new.id, old.revision as previous_revision
					""".formatted(type.table(), date),
					"insert into %s (%s, category_code, amount, quantity) values (?, ?, ?, ?)"
							.formatted(type.lineTable(), type.lineForeignKey()),
					"delete from %s where %s = :id".formatted(type.lineTable(), type.lineForeignKey()),
					"select revision from %s where branch_code = :branchCode and %s = :date".formatted(type.table(), date));
		}
	}

	private final JdbcClient jdbc;
	private final JdbcTemplate jdbcTemplate;
	private final Map<RecordType, Sql> sql = new EnumMap<>(RecordType.class);

	public DailyFiguresStore(JdbcClient jdbc, JdbcTemplate jdbcTemplate) {
		this.jdbc = jdbc;
		this.jdbcTemplate = jdbcTemplate;
		for (RecordType type : RecordType.values()) {
			sql.put(type, Sql.of(type));
		}
	}

	public Result apply(DailyFigures figures) {
		Sql statements = sql.get(figures.type());
		Optional<Written> written = jdbc.sql(statements.upsertHeader())
				.param("branchCode", figures.branchCode())
				.param("date", figures.date())
				.param("revision", figures.revision())
				.param("totalAmount", figures.totalAmount())
				.param("confirmedAt", figures.confirmedAt())
				.param("eventId", figures.eventId())
				.query((rs, n) -> new Written(rs.getLong("id"), rs.getObject("previous_revision", Integer.class)))
				.optional();

		if (written.isEmpty()) {
			int stored = jdbc.sql(statements.storedRevision())
					.param("branchCode", figures.branchCode())
					.param("date", figures.date())
					.query(Integer.class)
					.single();
			return new Result(stored == figures.revision() ? Outcome.DUPLICATE : Outcome.STALE, stored);
		}

		long headerId = written.get().id();
		jdbc.sql(statements.deleteLines()).param("id", headerId).update();
		List<Object[]> lines = figures.lines().stream()
				.map(l -> new Object[] { headerId, l.categoryCode(), l.amount(), l.quantity() })
				.toList();
		jdbcTemplate.batchUpdate(statements.insertLine(), lines);

		Outcome outcome = written.get().previousRevision() == null ? Outcome.INSERTED : Outcome.UPDATED;
		return new Result(outcome, figures.revision());
	}

	private record Written(long id, Integer previousRevision) {
	}
}
