/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.integration.elasticsearch.dsl;

import java.util.function.Supplier;

import org.springframework.data.elasticsearch.core.ReactiveElasticsearchOperations;
import org.springframework.data.elasticsearch.core.query.Query;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.expression.SupplierExpression;
import org.springframework.integration.expression.ValueExpression;

/**
 * The factory for Elasticsearch components in the Java DSL.
 *
 * @author Sharang Gupta
 *
 * @since 7.3
 */
public final class Elasticsearch {

	/**
	 * Create an {@link ElasticsearchMessageHandlerSpec} for an outbound channel adapter that indexes or
	 * deletes documents with the provided {@link ReactiveElasticsearchOperations}.
	 * @param elasticsearchOperations the operations to use
	 * @return the spec
	 */
	public static ElasticsearchMessageHandlerSpec outboundChannelAdapter(
			ReactiveElasticsearchOperations elasticsearchOperations) {

		return new ElasticsearchMessageHandlerSpec(elasticsearchOperations);
	}

	/**
	 * Create an {@link ElasticsearchMessageSourceSpec} for an inbound channel adapter that searches
	 * with a JSON String of the Elasticsearch Query DSL on every poll.
	 * @param elasticsearchOperations the operations to use
	 * @param query the JSON query
	 * @return the spec
	 */
	public static ElasticsearchMessageSourceSpec inboundChannelAdapter(
			ReactiveElasticsearchOperations elasticsearchOperations, String query) {

		return new ElasticsearchMessageSourceSpec(elasticsearchOperations, new LiteralExpression(query));
	}

	/**
	 * Create an {@link ElasticsearchMessageSourceSpec} for an inbound channel adapter that searches
	 * with a Spring Data Elasticsearch {@link Query} on every poll.
	 * Each poll emits the documents of the {@link Query#getPageable() pageable} of the query.
	 * @param elasticsearchOperations the operations to use
	 * @param query the query
	 * @return the spec
	 */
	public static ElasticsearchMessageSourceSpec inboundChannelAdapter(
			ReactiveElasticsearchOperations elasticsearchOperations, Query query) {

		return new ElasticsearchMessageSourceSpec(elasticsearchOperations, new ValueExpression<>(query));
	}

	/**
	 * Create an {@link ElasticsearchMessageSourceSpec} for an inbound channel adapter that searches
	 * with the {@link Query} of the supplier, called on every poll.
	 * Each poll emits the documents of the {@link Query#getPageable() pageable} of the query.
	 * @param elasticsearchOperations the operations to use
	 * @param querySupplier the query supplier
	 * @return the spec
	 */
	public static ElasticsearchMessageSourceSpec inboundChannelAdapter(
			ReactiveElasticsearchOperations elasticsearchOperations, Supplier<? extends Query> querySupplier) {

		return new ElasticsearchMessageSourceSpec(elasticsearchOperations, new SupplierExpression<>(querySupplier));
	}

	private Elasticsearch() {
	}

}
