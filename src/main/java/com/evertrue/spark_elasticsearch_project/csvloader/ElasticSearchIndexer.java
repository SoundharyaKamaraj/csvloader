package com.evertrue.spark_elasticsearch_project.csvloader;

import org.apache.spark.api.java.JavaRDD;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import static java.util.Objects.*;

/**
 * Abstract class for Data Ingestion to Elasticsearch cluster. 
 * Encapsulates all the logic of indexing a JavaRDD in Elasticsearch.
 * Extending class concrete class should given an index name to add the documents.
 * 
 * @author Soundharya
 *
 * @param <T>
 */
public abstract class ElasticSearchIndexer<T> {

	protected final String indexName;

	public ElasticSearchIndexer(String indexName) {
		this.indexName = requireNonNull(indexName, "indexName cannot be null");
	}

	public void index(JavaRDD<T> rdd) {
		JavaEsSpark.saveToEs(rdd, String.format("/%s", indexName));
	}

}
