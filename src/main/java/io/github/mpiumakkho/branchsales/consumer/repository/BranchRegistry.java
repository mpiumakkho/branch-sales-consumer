package io.github.mpiumakkho.branchsales.consumer.repository;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * Branches HQ reads from: rows of the branch registry with a Kafka address (set by infra/onboard-branch.sh, cleared by
 * infra/offboard-branch.sh).
 */
@Repository
public class BranchRegistry {

	private final JdbcClient jdbc;

	public BranchRegistry(JdbcClient jdbc) {
		this.jdbc = jdbc;
	}

	/** @return branch code to Kafka bootstrap address, in branch code order */
	public Map<String, String> kafkaAddresses() {
		Map<String, String> addresses = new LinkedHashMap<>();
		jdbc.sql("select branch_code, kafka_bootstrap from branch where kafka_bootstrap is not null order by branch_code")
				.query(rs -> {
					addresses.put(rs.getString("branch_code"), rs.getString("kafka_bootstrap"));
				});
		return addresses;
	}
}
