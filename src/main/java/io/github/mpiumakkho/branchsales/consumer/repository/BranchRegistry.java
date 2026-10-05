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

	/** @return branch code to Kafka bootstrap address of the branches in {@code shard}, in branch code order */
	public Map<String, String> kafkaAddresses(String shard) {
		Map<String, String> addresses = new LinkedHashMap<>();
		jdbc.sql("""
				select branch_code, kafka_bootstrap from branch
				 where kafka_bootstrap is not null and shard = :shard
				 order by branch_code
				""")
				.param("shard", shard)
				.query(rs -> {
					addresses.put(rs.getString("branch_code"), rs.getString("kafka_bootstrap"));
				});
		return addresses;
	}
}
