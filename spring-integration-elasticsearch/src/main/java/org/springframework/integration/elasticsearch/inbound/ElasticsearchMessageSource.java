/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.integration.elasticsearch.inbound;

import java.util.Map;

import org.jspecify.annotations.Nullable;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.core.ReactiveElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.Query;
import org.springframework.data.elasticsearch.core.query.StringQuery;
import org.springframework.expression.Expression;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.endpoint.AbstractMessageSource;
import org.springframework.util.Assert;

/**
 * A {@link org.springframework.integration.core.MessageSource} that searches Elasticsearch
 * with a {@link ReactiveElasticsearchOperations} on every {@link #receive()} call.
 * <p>
 * The payload of the produced message is a {@link Flux} of the documents that match the query,
 * mapped to the {@link #setPayloadType(Class) payload type} by Spring Data Elasticsearch.
 * The query is a JSON String of the Elasticsearch Query DSL or a {@link Query} evaluated
 * from the query expression on every {@link #receive()} call.
 * The search of a JSON String emits all the matching documents, while the search of a {@link Query}
 * emits the documents of its {@link Query#getPageable() pageable}, the first 10 by default.
 *
 * @author Sharang Gupta
 *
 * @since 7.3
 */
public class ElasticsearchMessageSource extends AbstractMessageSource<Publisher<?>> {

	private final ReactiveElasticsearchOperations elasticsearchOperations;

	private final Expression queryExpression;

	private Class<?> payloadType = Map.class;

	private @Nullable Expression indexNameExpression;

	/**
	 * Create an instance that searches with a JSON String of the Elasticsearch Query DSL.
	 * @param elasticsearchOperations the operations to search with
	 * @param query the JSON query, e.g. {@code {"match": {"title": "Refactoring"}}}
	 */
	public ElasticsearchMessageSource(ReactiveElasticsearchOperations elasticsearchOperations, String query) {
		this(elasticsearchOperations, new LiteralExpression(query));
	}

	/**
	 * Create an instance that searches with the result of the query expression,
	 * evaluated on every {@link #receive()} call: a {@link Query} or a JSON String
	 * of the Elasticsearch Query DSL.
	 * @param elasticsearchOperations the operations to search with
	 * @param queryExpression the query expression
	 */
	public ElasticsearchMessageSource(ReactiveElasticsearchOperations elasticsearchOperations,
			Expression queryExpression) {

		Assert.notNull(elasticsearchOperations, "'elasticsearchOperations' must not be null");
		Assert.notNull(queryExpression, "'queryExpression' must not be null");
		this.elasticsearchOperations = elasticsearchOperations;
		this.queryExpression = queryExpression;
	}

	/**
	 * Set the type the matching documents are mapped to. Defaults to {@link Map}.
	 * @param payloadType the payload type
	 */
	public void setPayloadType(Class<?> payloadType) {
		Assert.notNull(payloadType, "'payloadType' must not be null");
		this.payloadType = payloadType;
	}

	/**
	 * Set the name of the index to search.
	 * Required unless the payload type is mapped with a
	 * {@link Document @Document} annotation.
	 * @param indexName the index name
	 */
	public void setIndexName(String indexName) {
		Assert.hasText(indexName, "'indexName' must not be empty");
		setIndexNameExpression(new LiteralExpression(indexName));
	}

	/**
	 * Set an expression for the name of the index to search, evaluated on every {@link #receive()} call.
	 * @param indexNameExpression the index name expression
	 */
	public void setIndexNameExpression(Expression indexNameExpression) {
		Assert.notNull(indexNameExpression, "'indexNameExpression' must not be null");
		this.indexNameExpression = indexNameExpression;
	}

	@Override
	public String getComponentType() {
		return "elasticsearch:inbound-channel-adapter";
	}

	@Override
	protected void onInit() {
		super.onInit();
		Assert.state(this.indexNameExpression != null
						|| AnnotatedElementUtils.hasAnnotation(this.payloadType, Document.class),
				() -> "An index name is required to search for a payload type without a @Document mapping: "
						+ this.payloadType.getName());
	}

	@Override
	protected Object doReceive() {
		Query query = evaluateQuery();
		IndexCoordinates index = evaluateIndex();
		Flux<? extends SearchHit<?>> searchHits = (index != null)
				? this.elasticsearchOperations.search(query, this.payloadType, index)
				: this.elasticsearchOperations.search(query, this.payloadType);
		return searchHits.map(SearchHit::getContent);
	}

	private @Nullable IndexCoordinates evaluateIndex() {
		if (this.indexNameExpression == null) {
			return null;
		}
		String indexName = evaluateExpression(this.indexNameExpression, String.class);
		Assert.state(indexName != null, "'indexNameExpression' must not evaluate to null");
		return IndexCoordinates.of(indexName);
	}

	private Query evaluateQuery() {
		Object query = evaluateExpression(this.queryExpression);
		if (query instanceof Query elasticsearchQuery) {
			return elasticsearchQuery;
		}
		if (query instanceof String json) {
			return new StringQuery(json, Pageable.unpaged());
		}
		throw new IllegalStateException(
				"The 'queryExpression' must evaluate to a Query or a JSON String, but it evaluated to: " + query);
	}

}
