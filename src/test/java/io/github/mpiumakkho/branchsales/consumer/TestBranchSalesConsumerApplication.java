package io.github.mpiumakkho.branchsales.consumer;

import org.springframework.boot.SpringApplication;

public class TestBranchSalesConsumerApplication {

	public static void main(String[] args) {
		SpringApplication.from(BranchSalesConsumerApplication::main).with(TestcontainersConfiguration.class).run(args);
	}

}
