/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.integration.elasticsearch.dsl;

import org.springframework.data.elasticsearch.core.ReactiveElasticsearchOperations;

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

	private Elasticsearch() {
	}

}
