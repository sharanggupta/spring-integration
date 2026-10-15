/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.integration.elasticsearch.inbound;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.core.ReactiveElasticsearchOperations;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.Criteria;
import org.springframework.data.elasticsearch.core.query.CriteriaQuery;
import org.springframework.integration.elasticsearch.ElasticsearchContainerTest;
import org.springframework.integration.expression.SupplierExpression;
import org.springframework.integration.expression.ValueExpression;
import org.springframework.integration.test.support.TestApplicationContextAware;
import org.springframework.messaging.Message;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.InstanceOfAssertFactories.MAP;

/**
 * @author Sharang Gupta
 *
 * @since 7.3
 */
class ElasticsearchMessageSourceTests implements ElasticsearchContainerTest, TestApplicationContextAware {

	private static final Duration TIMEOUT = Duration.ofSeconds(30);

	private static final String MATCH_ALL = "{\"match_all\": {}}";

	private static ReactiveElasticsearchOperations operations;

	@BeforeAll
	static void indexTheCatalog() {
		operations = ElasticsearchContainerTest.reactiveElasticsearchOperations();
		operations.saveAll(List.of(new Book("ddd", "Domain-Driven Design"), new Book("refactoring", "Refactoring")),
				Book.class).blockLast(TIMEOUT);
		operations.indexOps(Book.class).refresh().block(TIMEOUT);
	}

	@Test
	void emitsTheEntitiesThatMatchAJsonQuery() {
		ElasticsearchMessageSource source =
				new ElasticsearchMessageSource(operations, "{\"match\": {\"title\": \"Refactoring\"}}");
		source.setPayloadType(Book.class);

		StepVerifier.create(payloadOf(initialized(source)))
				.expectNext(new Book("refactoring", "Refactoring"))
				.verifyComplete();
	}

	@Test
	void emitsTheEntitiesThatMatchTheQueryOfAQueryExpression() {
		ElasticsearchMessageSource source = new ElasticsearchMessageSource(operations,
				new SupplierExpression<>(() -> new CriteriaQuery(Criteria.where("title").is("Domain-Driven Design"))));
		source.setPayloadType(Book.class);

		StepVerifier.create(payloadOf(initialized(source)))
				.expectNext(new Book("ddd", "Domain-Driven Design"))
				.verifyComplete();
	}

	@Test
	void emitsMapsOfTheIndexByDefault() {
		ElasticsearchMessageSource source = new ElasticsearchMessageSource(operations, MATCH_ALL);
		source.setIndexName("catalog");

		StepVerifier.create(payloadOf(initialized(source)))
				.assertNext(document -> assertThat(document).asInstanceOf(MAP).containsKey("title"))
				.assertNext(document -> assertThat(document).asInstanceOf(MAP).containsKey("title"))
				.verifyComplete();
	}

	@Test
	void emitsAllTheMatchingDocumentsAndNotOnlyTheFirstPage() {
		IndexCoordinates periodicals = IndexCoordinates.of("periodicals");
		operations.saveAll(IntStream.range(0, 12).mapToObj(issue -> new HashMap<>(Map.of("issue", issue))).toList(), periodicals)
				.blockLast(TIMEOUT);
		operations.indexOps(periodicals).refresh().block(TIMEOUT);
		ElasticsearchMessageSource source = new ElasticsearchMessageSource(operations, MATCH_ALL);
		source.setIndexName("periodicals");

		StepVerifier.create(payloadOf(initialized(source)))
				.expectNextCount(12)
				.verifyComplete();
	}

	@Test
	void searchesWithTheSameQueryOnEveryReceive() {
		ElasticsearchMessageSource source = new ElasticsearchMessageSource(operations,
				new ValueExpression<>(new CriteriaQuery(Criteria.where("title").is("Refactoring"))));
		source.setPayloadType(Book.class);
		initialized(source);

		StepVerifier.create(payloadOf(source))
				.expectNext(new Book("refactoring", "Refactoring"))
				.verifyComplete();
		StepVerifier.create(payloadOf(source))
				.expectNext(new Book("refactoring", "Refactoring"))
				.verifyComplete();
	}

	@Test
	void requiresAnIndexNameForAPayloadTypeWithoutDocumentMapping() {
		ElasticsearchMessageSource source = new ElasticsearchMessageSource(operations, MATCH_ALL);
		source.setBeanFactory(TEST_INTEGRATION_CONTEXT);

		assertThatIllegalStateException().isThrownBy(source::afterPropertiesSet)
				.withMessageContaining("An index name is required");
	}

	@Test
	void rejectsAQueryExpressionThatEvaluatesToNeitherAQueryNorAString() {
		ElasticsearchMessageSource source = new ElasticsearchMessageSource(operations,
				new SupplierExpression<>(() -> 42));
		source.setPayloadType(Book.class);

		assertThatIllegalStateException().isThrownBy(initialized(source)::receive)
				.withMessageContaining("must evaluate to a Query or a JSON String");
	}

	@SuppressWarnings("unchecked")
	private static Flux<Object> payloadOf(ElasticsearchMessageSource source) {
		Message<?> message = source.receive();
		assertThat(message).isNotNull();
		return (Flux<Object>) message.getPayload();
	}

	private static ElasticsearchMessageSource initialized(ElasticsearchMessageSource source) {
		source.setBeanFactory(TEST_INTEGRATION_CONTEXT);
		source.afterPropertiesSet();
		return source;
	}

	@Document(indexName = "catalog")
	record Book(@Id String id, String title) {

	}

}
