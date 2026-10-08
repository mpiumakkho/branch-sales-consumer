package io.github.mpiumakkho.branchsales.consumer.repository;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.List;

import org.jspecify.annotations.Nullable;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import io.github.mpiumakkho.branchsales.consumer.dto.DailyFigures;
import io.github.mpiumakkho.branchsales.consumer.dto.RecordType;
import io.github.mpiumakkho.branchsales.consumer.exception.RejectReason;
import io.github.mpiumakkho.branchsales.consumer.exception.RejectedMessageException;

/**
 * Rejected summary records (table dead_letter), kept unchanged so they can be inspected and replayed.
 */
@Repository
public class DeadLetterStore {

	public record DeadLetter(long id, RecordType type, String branchCode, long sourceOffset,
			@Nullable String recordKey, byte @Nullable [] recordValue, OffsetDateTime replayRequestedAt) {
	}

	private final JdbcClient jdbc;

	public DeadLetterStore(JdbcClient jdbc) {
		this.jdbc = jdbc;
	}

	/**
	 * Keeps a rejected record. A record read again (same branch, type and offset) is kept once: the first row, and
	 * any replay state on it, stays as it is. The business date is kept when the record could be read, so the
	 * returns waiting for a day's sales can be found.
	 */
	public void save(RecordType type, String branchCode, long sourceOffset, @Nullable String key,
			byte @Nullable [] value, RejectedMessageException rejection) {
		DailyFigures figures = rejection.figures();
		jdbc.sql("""
				insert into dead_letter (branch_code, record_type, source_offset, record_key, record_value, reject_reason,
				                         detail, record_date)
				values (:branchCode, :type, :sourceOffset, :key, :value, :reason, :detail, :date)
				on conflict (branch_code, record_type, source_offset) do nothing
				""")
				.param("branchCode", branchCode)
				.param("type", type.name())
				.param("sourceOffset", sourceOffset)
				.param("key", key)
				.param("value", value)
				.param("reason", rejection.reason().name())
				.param("detail", rejection.getMessage())
				.param("date", figures == null ? null : figures.date())
				.update();
	}

	/**
	 * The daily sales of a branch and date were stored: ask for the replay of that date's returns that were
	 * rejected because the sales were missing. Runs in the caller's transaction.
	 * @return how many rows were marked
	 */
	public int requestReplayOfReturnsWaitingFor(String branchCode, LocalDate date) {
		return jdbc.sql("""
				update dead_letter
				   set replay_requested_at = now()
				 where branch_code = :branchCode
				   and record_type = :type
				   and record_date = :date
				   and reject_reason = :reason
				   and (replayed_at is null or replay_result = 'REJECTED')
				""")
				.param("branchCode", branchCode)
				.param("type", RecordType.DAILY_RETURN.name())
				.param("date", date)
				.param("reason", RejectReason.PARENT_MISSING.name())
				.update();
	}

	/** Rejected records that have not been replayed successfully: still waiting for a fix at HQ (metrics). */
	public long countOpen() {
		return jdbc.sql("select count(*) from dead_letter where replay_result is null or replay_result = 'REJECTED'")
				.query(Long.class)
				.single();
	}

	/**
	 * Rows of the given branches whose replay was requested after their last replay, oldest first. Each consumer
	 * instance passes the branches it is connected to, so a row is replayed by the instance that can send the receipt.
	 */
	public List<DeadLetter> findReplayRequested(int limit, Collection<String> branches) {
		if (branches.isEmpty()) {
			return List.of();
		}
		return jdbc.sql("""
				select id, record_type, branch_code, source_offset, record_key, record_value, replay_requested_at
				  from dead_letter
				 where replay_requested_at is not null
				   and (replayed_at is null or replayed_at < replay_requested_at)
				   and branch_code in (:branches)
				 order by id
				 limit :limit
				""")
				.param("branches", branches)
				.param("limit", limit)
				.query((rs, n) -> new DeadLetter(rs.getLong("id"), RecordType.valueOf(rs.getString("record_type")),
						rs.getString("branch_code"), rs.getLong("source_offset"), rs.getString("record_key"),
						rs.getBytes("record_value"), rs.getObject("replay_requested_at", OffsetDateTime.class)))
				.list();
	}

	/** The replay was accepted: {@code result} is the stored outcome (INSERTED, UPDATED, DUPLICATE, STALE). */
	public void markReplayed(long id, String result) {
		jdbc.sql("update dead_letter set replayed_at = now(), replay_result = :result where id = :id")
				.param("result", result)
				.param("id", id)
				.update();
	}

	/** The replay was rejected again, possibly for another reason. */
	public void markReplayRejected(long id, RejectedMessageException rejection) {
		jdbc.sql("""
				update dead_letter
				   set replayed_at = now(), replay_result = 'REJECTED', reject_reason = :reason, detail = :detail
				 where id = :id
				""")
				.param("reason", rejection.reason().name())
				.param("detail", rejection.getMessage())
				.param("id", id)
				.update();
	}
}
