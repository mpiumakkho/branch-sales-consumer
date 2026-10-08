package io.github.mpiumakkho.branchsales.consumer.repository;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.github.mpiumakkho.branchsales.consumer.dto.RecordType;

/** The statements built from the RecordType enum; the database behaviour is covered by the flow tests. */
class DailyFiguresStoreSqlTest {

	@Test
	void dailyTypesKeepTheirStatements() {
		DailyFiguresStore.Sql sales = DailyFiguresStore.Sql.of(RecordType.DAILY_SUMMARY);
		assertThat(sales.upsertHeader()).isEqualTo("""
				insert into branch_daily_sales (branch_code, sale_date, revision, total_amount, confirmed_at, event_id)
				values (:branchCode, :date, :revision, :totalAmount, :confirmedAt, :eventId)
				on conflict (branch_code, sale_date) do update
				   set revision     = excluded.revision,
				       total_amount = excluded.total_amount,
				       confirmed_at = excluded.confirmed_at,
				       event_id     = excluded.event_id,
				       received_at  = now()
				 where branch_daily_sales.revision < excluded.revision
				returning new.id, old.revision as previous_revision
				""");
		assertThat(sales.insertLine()).isEqualTo(
				"insert into branch_daily_sales_line (branch_daily_sales_id, category_code, amount, quantity) values (?, ?, ?, ?)");
		assertThat(sales.deleteLines()).isEqualTo("delete from branch_daily_sales_line where branch_daily_sales_id = :id");
		assertThat(sales.storedRevision()).isEqualTo(
				"select revision from branch_daily_sales where branch_code = :branchCode and sale_date = :date");

		DailyFiguresStore.Sql returns = DailyFiguresStore.Sql.of(RecordType.DAILY_RETURN);
		assertThat(returns.upsertHeader())
				.contains("insert into branch_daily_return (branch_code, return_date, revision,")
				.contains("on conflict (branch_code, return_date) do update")
				.contains(" where branch_daily_return.revision < excluded.revision");
		assertThat(returns.insertLine()).isEqualTo(
				"insert into branch_daily_return_line (branch_daily_return_id, category_code, amount, quantity) values (?, ?, ?, ?)");
		assertThat(returns.storedRevision()).isEqualTo(
				"select revision from branch_daily_return where branch_code = :branchCode and return_date = :date");
	}

	@Test
	void shiftCloseUsesItsFourColumnKeyAndTenderLines() {
		DailyFiguresStore.Sql shift = DailyFiguresStore.Sql.of(RecordType.SHIFT_CLOSE);
		assertThat(shift.upsertHeader())
				.startsWith("insert into branch_shift_close (branch_code, business_date, terminal_id, shift_no, revision,")
				.contains("on conflict (branch_code, business_date, terminal_id, shift_no) do update")
				.contains("cash_counted      = excluded.cash_counted,")
				.contains(" where branch_shift_close.revision < excluded.revision")
				.contains("returning new.id, old.revision as previous_revision")
				// generated at HQ, never written
				.doesNotContain("cash_over_short");
		assertThat(shift.insertLine()).isEqualTo(
				"insert into branch_shift_close_tender (branch_shift_close_id, tender_type, amount, quantity) values (?, ?, ?, ?)");
		assertThat(shift.deleteLines()).isEqualTo("delete from branch_shift_close_tender where branch_shift_close_id = :id");
		assertThat(shift.storedRevision())
				.startsWith("select revision from branch_shift_close")
				.contains("business_date = :date")
				.contains("terminal_id = :terminalId")
				.contains("shift_no = :shiftNo");
	}
}
