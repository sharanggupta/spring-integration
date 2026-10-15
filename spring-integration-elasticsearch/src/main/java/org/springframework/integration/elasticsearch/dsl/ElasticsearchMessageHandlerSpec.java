/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.integration.elasticsearch.dsl;

import java.util.function.Function;

import org.springframework.data.elasticsearch.core.ReactiveElasticsearchOperations;
import org.springframework.expression.Expression;
import org.springframework.integration.dsl.ReactiveMessageHandlerSpec;
import org.springframework.integration.elasticsearch.outbound.ElasticsearchMessageHandler;
import org.springframework.integration.expression.FunctionExpression;
import org.springframework.messaging.Message;

/**
 * The {@link ReactiveMessageHandlerSpec} for the {@link ElasticsearchMessageHandler}.
 *
 * @author Sharang Gupta
 *
 * @since 7.3
 */
public class ElasticsearchMessageHandlerSpec
		extends ReactiveMessageHandlerSpec<ElasticsearchMessageHandlerSpec, ElasticsearchMessageHandler> {

	protected ElasticsearchMessageHandlerSpec(ReactiveElasticsearchOperations elasticsearchOperations) {
		super(new ElasticsearchMessageHandler(elasticsearchOperations));
	}

	/**
	 * Set the operation to perform for each message.
	 * @param type the operation to perform
	 * @return the spec
	 * @see ElasticsearchMessageHandler#setType(ElasticsearchMessageHandler.Type)
	 */
	public ElasticsearchMessageHandlerSpec type(ElasticsearchMessageHandler.Type type) {
		this.reactiveMessageHandler.setType(type);
		return this;
	}

	/**
	 * Set a function to evaluate the operation to perform for each message.
	 * @param typeFunction the function to use
	 * @param <P> the payload type
	 * @return the spec
	 * @see ElasticsearchMessageHandler#setTypeExpression(Expression)
	 */
	public <P> ElasticsearchMessageHandlerSpec typeFunction(
			Function<Message<P>, ElasticsearchMessageHandler.Type> typeFunction) {

		return typeExpression(new FunctionExpression<>(typeFunction));
	}

	/**
	 * Set a SpEL expression to evaluate the operation to perform for each message.
	 * @param typeExpression the expression to use
	 * @return the spec
	 * @see ElasticsearchMessageHandler#setTypeExpression(Expression)
	 */
	public ElasticsearchMessageHandlerSpec typeExpression(String typeExpression) {
		return typeExpression(PARSER.parseExpression(typeExpression));
	}

	/**
	 * Set a SpEL expression to evaluate the operation to perform for each message.
	 * @param typeExpression the expression to use
	 * @return the spec
	 * @see ElasticsearchMessageHandler#setTypeExpression(Expression)
	 */
	public ElasticsearchMessageHandlerSpec typeExpression(Expression typeExpression) {
		this.reactiveMessageHandler.setTypeExpression(typeExpression);
		return this;
	}

	/**
	 * Set the name of the index for the documents.
	 * @param indexName the name of the index
	 * @return the spec
	 * @see ElasticsearchMessageHandler#setIndexName(String)
	 */
	public ElasticsearchMessageHandlerSpec indexName(String indexName) {
		this.reactiveMessageHandler.setIndexName(indexName);
		return this;
	}

	/**
	 * Set a function to evaluate the name of the index for each message.
	 * @param indexNameFunction the function to use
	 * @param <P> the payload type
	 * @return the spec
	 * @see ElasticsearchMessageHandler#setIndexNameExpression(Expression)
	 */
	public <P> ElasticsearchMessageHandlerSpec indexNameFunction(Function<Message<P>, String> indexNameFunction) {
		return indexNameExpression(new FunctionExpression<>(indexNameFunction));
	}

	/**
	 * Set a SpEL expression to evaluate the name of the index for each message.
	 * @param indexNameExpression the expression to use
	 * @return the spec
	 * @see ElasticsearchMessageHandler#setIndexNameExpression(Expression)
	 */
	public ElasticsearchMessageHandlerSpec indexNameExpression(String indexNameExpression) {
		return indexNameExpression(PARSER.parseExpression(indexNameExpression));
	}

	/**
	 * Set a SpEL expression to evaluate the name of the index for each message.
	 * @param indexNameExpression the expression to use
	 * @return the spec
	 * @see ElasticsearchMessageHandler#setIndexNameExpression(Expression)
	 */
	public ElasticsearchMessageHandlerSpec indexNameExpression(Expression indexNameExpression) {
		this.reactiveMessageHandler.setIndexNameExpression(indexNameExpression);
		return this;
	}

	/**
	 * Set a function to evaluate the id of the document to delete for each message.
	 * @param idFunction the function to use
	 * @param <P> the payload type
	 * @return the spec
	 * @see ElasticsearchMessageHandler#setIdExpression(Expression)
	 */
	public <P> ElasticsearchMessageHandlerSpec idFunction(Function<Message<P>, String> idFunction) {
		return idExpression(new FunctionExpression<>(idFunction));
	}

	/**
	 * Set a SpEL expression to evaluate the id of the document to delete for each message.
	 * @param idExpression the expression to use
	 * @return the spec
	 * @see ElasticsearchMessageHandler#setIdExpression(Expression)
	 */
	public ElasticsearchMessageHandlerSpec idExpression(String idExpression) {
		return idExpression(PARSER.parseExpression(idExpression));
	}

	/**
	 * Set a SpEL expression to evaluate the id of the document to delete for each message.
	 * @param idExpression the expression to use
	 * @return the spec
	 * @see ElasticsearchMessageHandler#setIdExpression(Expression)
	 */
	public ElasticsearchMessageHandlerSpec idExpression(Expression idExpression) {
		this.reactiveMessageHandler.setIdExpression(idExpression);
		return this;
	}

}
