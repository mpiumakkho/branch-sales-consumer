package io.github.mpiumakkho.branchsales.consumer.validation;

import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

import io.github.mpiumakkho.branchsales.consumer.summary.DailySalesSummary;
import io.github.mpiumakkho.branchsales.consumer.summary.DailySalesSummary.SalesLine;

/**
 * Reference layer of the contract checks: branch and categories must exist in
 * the HQ tables.
 */
@Component
public class ReferenceChecker {

	private final JdbcClient jdbc;

	public ReferenceChecker(JdbcClient jdbc) {
		this.jdbc = jdbc;
	}

	/**
	 * @throws RejectedMessageException with {@link RejectReason#UNKNOWN_BRANCH} or {@link RejectReason#UNKNOWN_CATEGORY}
	 */
	public void check(DailySalesSummary summary) {
		boolean branchExists = jdbc.sql("select exists (select 1 from branch where branch_code = :code)")
				.param("code", summary.branchCode())
				.query(Boolean.class)
				.single();
		if (!branchExists) {
			throw new RejectedMessageException(RejectReason.UNKNOWN_BRANCH,
					"branchCode " + summary.branchCode() + " is not in the branch registry");
		}

		Set<String> codes = summary.lines().stream().map(SalesLine::categoryCode).collect(Collectors.toSet());
		Set<String> unknown = new TreeSet<>(codes);
		unknown.removeAll(jdbc.sql("select category_code from category where category_code in (:codes)")
				.param("codes", codes)
				.query(String.class)
				.set());
		if (!unknown.isEmpty()) {
			throw new RejectedMessageException(RejectReason.UNKNOWN_CATEGORY,
					"categoryCode " + unknown + " is not in the category table");
		}
	}
}
