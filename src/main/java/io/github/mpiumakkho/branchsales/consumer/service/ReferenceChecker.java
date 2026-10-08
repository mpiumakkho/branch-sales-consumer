package io.github.mpiumakkho.branchsales.consumer.service;

import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

import io.github.mpiumakkho.branchsales.consumer.dto.DailyFigures;
import io.github.mpiumakkho.branchsales.consumer.dto.DailyFigures.Line;
import io.github.mpiumakkho.branchsales.consumer.dto.RecordType;
import io.github.mpiumakkho.branchsales.consumer.exception.RejectReason;
import io.github.mpiumakkho.branchsales.consumer.exception.RejectedMessageException;

/**
 * Reference layer of the contract checks: the branch and the line codes (categories, or tenders of a shift close)
 * must exist in the HQ tables, and a return needs the branch's daily sales of the same date (its parent) to be
 * stored already.
 */
@Component
public class ReferenceChecker {

	private final JdbcClient jdbc;

	public ReferenceChecker(JdbcClient jdbc) {
		this.jdbc = jdbc;
	}

	/**
	 * @throws RejectedMessageException with {@link RejectReason#UNKNOWN_BRANCH}, the type's
	 *                                  {@link RecordType.LineShape#unknownReason()} or {@link RejectReason#PARENT_MISSING}
	 */
	public void check(DailyFigures figures) {
		boolean branchExists = jdbc.sql("select exists (select 1 from branch where branch_code = :code)")
				.param("code", figures.branchCode())
				.query(Boolean.class)
				.single();
		if (!branchExists) {
			throw new RejectedMessageException(RejectReason.UNKNOWN_BRANCH,
					"branchCode " + figures.branchCode() + " is not in the branch registry", figures);
		}

		RecordType.LineShape shape = figures.type().lineShape();
		Set<String> codes = figures.lines().stream().map(Line::code).collect(Collectors.toSet());
		if (!codes.isEmpty()) {
			Set<String> unknown = new TreeSet<>(codes);
			// Table and column names come from the RecordType enum, not from input
			unknown.removeAll(jdbc.sql("select %1$s from %2$s where %1$s in (:codes)"
					.formatted(shape.codeColumn(), shape.referenceTable()))
					.param("codes", codes)
					.query(String.class)
					.set());
			if (!unknown.isEmpty()) {
				throw new RejectedMessageException(shape.unknownReason(),
						shape.codeField() + " " + unknown + " is not in the " + shape.referenceTable() + " table", figures);
			}
		}

		if (figures.type() == RecordType.DAILY_RETURN) {
			boolean parentExists = jdbc.sql(
					"select exists (select 1 from branch_daily_sales where branch_code = :code and sale_date = :date)")
					.param("code", figures.branchCode())
					.param("date", figures.date())
					.query(Boolean.class)
					.single();
			if (!parentExists) {
				throw new RejectedMessageException(RejectReason.PARENT_MISSING, "no daily sales of "
						+ figures.branchCode() + " for " + figures.date() + " at HQ yet; replayed when they arrive",
						figures);
			}
		}
	}
}
