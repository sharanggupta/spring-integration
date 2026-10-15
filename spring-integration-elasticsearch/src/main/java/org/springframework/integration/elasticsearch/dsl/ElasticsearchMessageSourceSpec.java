/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.integration.elasticsearch.dsl;

import java.util.function.Supplier;

import org.springframework.data.elasticsearch.core.ReactiveElasticsearchOperations;
import org.springframework.expression.Expression;
import org.springframework.integration.dsl.MessageSourceSpec;
import org.springframework.integration.elasticsearch.inbound.ElasticsearchMessageSource;
import org.springframework.integration.expression.SupplierExpression;

/**
 * The {@link MessageSourceSpec} for the {@link ElasticsearchMessageSource}.
 *
 * @author Sharang Gupta
 *
 * @since 7.3
 */
public class ElasticsearchMessageSourceSpec
		extends MessageSourceSpec<ElasticsearchMessageSourceSpec, ElasticsearchMessageSource> {

	protected ElasticsearchMessageSourceSpec(ReactiveElasticsearchOperations elasticsearchOperations,
			Expression queryExpression) {

		this.target = new ElasticsearchMessageSource(elasticsearchOperations, queryExpression);
	}

	/**
	 * Set the type the matching documents are mapped to.
	 * @param payloadType the payload type
	 * @return the spec
	 * @see ElasticsearchMessageSource#setPayloadType(Class)
	 */
	public ElasticsearchMessageSourceSpec payloadType(Class<?> payloadType) {
		this.target.setPayloadType(payloadType);
		return this;
	}

	/**
	 * Set the name of the index to search.
	 * @param indexName the index name
	 * @return the spec
	 * @see ElasticsearchMessageSource#setIndexName(String)
	 */
	public ElasticsearchMessageSourceSpec indexName(String indexName) {
		this.target.setIndexName(indexName);
		return this;
	}

	/**
	 * Set a {@link Supplier} for the name of the index to search, called on every {@code receive()}.
	 * @param indexNameSupplier the index name supplier
	 * @return the spec
	 */
	public ElasticsearchMessageSourceSpec indexNameSupplier(Supplier<String> indexNameSupplier) {
		return indexNameExpression(new SupplierExpression<>(indexNameSupplier));
	}

	/**
	 * Set a SpEL expression for the name of the index to search, evaluated on every {@code receive()}.
	 * @param indexNameExpression the index name expression
	 * @return the spec
	 */
	public ElasticsearchMessageSourceSpec indexNameExpression(String indexNameExpression) {
		return indexNameExpression(PARSER.parseExpression(indexNameExpression));
	}

	/**
	 * Set an expression for the name of the index to search, evaluated on every {@code receive()}.
	 * @param indexNameExpression the index name expression
	 * @return the spec
	 * @see ElasticsearchMessageSource#setIndexNameExpression(Expression)
	 */
	public ElasticsearchMessageSourceSpec indexNameExpression(Expression indexNameExpression) {
		this.target.setIndexNameExpression(indexNameExpression);
		return this;
	}

}
