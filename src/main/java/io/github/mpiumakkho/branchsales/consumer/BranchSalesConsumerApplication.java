package io.github.mpiumakkho.branchsales.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@ConfigurationPropertiesScan
// Branch registry refresh and dead-letter replay
@EnableScheduling
public class BranchSalesConsumerApplication {

	public static void main(String[] args) {
		SpringApplication.run(BranchSalesConsumerApplication.class, args);
	}

}
