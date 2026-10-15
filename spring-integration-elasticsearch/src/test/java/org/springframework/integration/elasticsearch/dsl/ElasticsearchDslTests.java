/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.integration.elasticsearch.dsl;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.core.ReactiveElasticsearchOperations;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.integration.elasticsearch.ElasticsearchContainerTest;
import org.springframework.integration.elasticsearch.outbound.ElasticsearchMessageHandler;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.MessageChannel;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.awaitility.Awaitility.await;

/**
 * @author Sharang Gupta
 *
 * @since 7.3
 */
@SpringJUnitConfig
@DirtiesContext
class ElasticsearchDslTests implements ElasticsearchContainerTest {

	private static final Duration TIMEOUT = Duration.ofSeconds(30);

	@Autowired
	private ReactiveElasticsearchOperations operations;

	@Autowired
	@Qualifier("elasticsearchFlow.input")
	private MessageChannel elasticsearchFlowInput;

	@Test
	void indexesAndDeletesThroughTheJavaDsl() {
		this.elasticsearchFlowInput.send(MessageBuilder.withPayload(new Magazine("byte", "Byte")).build());

		await().ignoreExceptions().until(() -> Boolean.TRUE.equals(this.operations.exists("byte", Magazine.class).block(TIMEOUT)));

		this.elasticsearchFlowInput.send(MessageBuilder.withPayload(new Magazine("byte", "Byte"))
				.setHeader("withdrawn", true)
				.build());

		await().ignoreExceptions().until(() -> Boolean.FALSE.equals(this.operations.exists("byte", Magazine.class).block(TIMEOUT)));
	}

	@Configuration
	@EnableIntegration
	static class ElasticsearchFlowConfiguration {

		@Bean
		ReactiveElasticsearchOperations reactiveElasticsearchOperations() {
			return ElasticsearchContainerTest.reactiveElasticsearchOperations();
		}

		@Bean
		IntegrationFlow elasticsearchFlow(ReactiveElasticsearchOperations reactiveElasticsearchOperations) {
			return flow -> flow
					.channel(MessageChannels.flux())
					.handleReactive(Elasticsearch.outboundChannelAdapter(reactiveElasticsearchOperations)
							.typeFunction(message -> message.getHeaders().containsKey("withdrawn")
									? ElasticsearchMessageHandler.Type.DELETE
									: ElasticsearchMessageHandler.Type.INDEX)
							.indexName("magazines"));
		}

	}

	@Document(indexName = "magazines")
	record Magazine(@Id String id, String title) {

	}

}
