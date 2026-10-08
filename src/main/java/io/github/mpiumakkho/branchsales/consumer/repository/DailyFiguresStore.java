package io.github.mpiumakkho.branchsales.consumer.repository;

import java.sql.Types;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import io.github.mpiumakkho.branchsales.consumer.dto.DailyFigures;
import io.github.mpiumakkho.branchsales.consumer.dto.DailyFigures.ShiftDetail;
import io.github.mpiumakkho.branchsales.consumer.dto.RecordKey.DayKey;
import io.github.mpiumakkho.branchsales.consumer.dto.RecordKey.ShiftKey;
import io.github.mpiumakkho.branchsales.consumer.dto.RecordType;

/**
 * Stores a record by revision (rules R4–R6) in the tables of its type. Must run inside a transaction so the header
 * and its lines change together.
 */
@Repository
public class DailyFiguresStore {

	public enum Outcome {
		/** No row for (type, branchCode, key) yet. */
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

	/** The statements of one record type. Package-private for the template test. */
	record Sql(String upsertHeader, String insertLine, String deleteLines, String storedRevision) {

		// The WHERE on DO UPDATE makes the revision check and the write one atomic statement, so two consumers
		// handling the same key during a rebalance cannot overwrite a higher revision.
		// RETURNING old.* needs PostgreSQL 18; old.revision is null when the row was inserted.
		// Table and column names come from the RecordType enum, not from input.
		static Sql of(RecordType type) {
			String insertLine = "insert into %s (%s, %s, amount, quantity) values (?, ?, ?, ?)"
					.formatted(type.lineTable(), type.lineForeignKey(), type.lineShape().codeColumn());
			String deleteLines = "delete from %s where %s = :id".formatted(type.lineTable(), type.lineForeignKey());
			return switch (type.keyShape().kind()) {
				case DAY -> day(type, insertLine, deleteLines);
				case SHIFT -> shift(type, insertLine, deleteLines);
			};
		}

		private static Sql day(RecordType type, String insertLine, String deleteLines) {
			String date = type.keyShape().dateColumn();
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
					insertLine,
					deleteLines,
					"select revision from %s where branch_code = :branchCode and %s = :date".formatted(type.table(), date));
		}

		// Key (branch_code, business_date, terminal_id, shift_no); the header also holds the shift's own fields
		private static Sql shift(RecordType type, String insertLine, String deleteLines) {
			String date = type.keyShape().dateColumn();
			return new Sql(
					"""
					insert into %1$s (branch_code, %2$s, terminal_id, shift_no, revision, cashier_id, opened_at, closed_at,
					                  transaction_count, total_amount, cash_expected, cash_counted, confirmed_at, event_id)
					values (:branchCode, :date, :terminalId, :shiftNo, :revision, :cashierId, :openedAt, :closedAt,
					        :transactionCount, :totalAmount, :cashExpected, :cashCounted, :confirmedAt, :eventId)
					on conflict (branch_code, %2$s, terminal_id, shift_no) do update
					   set revision          = excluded.revision,
					       cashier_id        = excluded.cashier_id,
					       opened_at         = excluded.opened_at,
					       closed_at         = excluded.closed_at,
					       transaction_count = excluded.transaction_count,
					       total_amount      = excluded.total_amount,
					       cash_expected     = excluded.cash_expected,
					       cash_counted      = excluded.cash_counted,
					       confirmed_at      = excluded.confirmed_at,
					       event_id          = excluded.event_id,
					       received_at       = now()
					 where %1$s.revision < excluded.revision
					returning new.id, old.revision as previous_revision
					""".formatted(type.table(), date),
					insertLine,
					deleteLines,
					"""
					select revision from %s
					 where branch_code = :branchCode and %s = :date and terminal_id = :terminalId and shift_no = :shiftNo"""
							.formatted(type.table(), date));
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
		Optional<Written> written = keyParams(jdbc.sql(statements.upsertHeader()), figures)
				.param("revision", figures.revision())
				.param("totalAmount", figures.totalAmount())
				.param("confirmedAt", figures.confirmedAt())
				.param("eventId", figures.eventId())
				.query((rs, n) -> new Written(rs.getLong("id"), rs.getObject("previous_revision", Integer.class)))
				.optional();

		if (written.isEmpty()) {
			int stored = keyParams(jdbc.sql(statements.storedRevision()), figures)
					.query(Integer.class)
					.single();
			return new Result(stored == figures.revision() ? Outcome.DUPLICATE : Outcome.STALE, stored);
		}

		long headerId = written.get().id();
		jdbc.sql(statements.deleteLines()).param("id", headerId).update();
		List<Object[]> lines = figures.lines().stream()
				.map(l -> new Object[] { headerId, l.code(), l.amount(), l.quantity() })
				.toList();
		if (!lines.isEmpty()) {
			jdbcTemplate.batchUpdate(statements.insertLine(), lines);
		}

		Outcome outcome = written.get().previousRevision() == null ? Outcome.INSERTED : Outcome.UPDATED;
		return new Result(outcome, figures.revision());
	}

	/**
	 * Binds branchCode and the key of the record. For a shift close, also its own header fields: a parameter the
	 * statement does not name is ignored, so the stored-revision query can share this.
	 */
	private static JdbcClient.StatementSpec keyParams(JdbcClient.StatementSpec statement, DailyFigures figures) {
		statement.param("branchCode", figures.branchCode()).param("date", figures.date());
		return switch (figures.key()) {
			case DayKey day -> statement;
			case ShiftKey shift -> {
				ShiftDetail detail = Objects.requireNonNull(figures.shift(), "shift close without its shift fields");
				yield statement
						.param("terminalId", shift.terminalId())
						.param("shiftNo", shift.shiftNo())
						.param("cashierId", detail.cashierId(), Types.VARCHAR)
						.param("openedAt", detail.openedAt())
						.param("closedAt", detail.closedAt())
						.param("transactionCount", detail.transactionCount())
						.param("cashExpected", detail.cashExpected())
						.param("cashCounted", detail.cashCounted());
			}
		};
	}

	private record Written(long id, Integer previousRevision) {
	}
}
