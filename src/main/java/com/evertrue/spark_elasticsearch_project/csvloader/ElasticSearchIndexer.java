package com.evertrue.spark_elasticsearch_project.csvloader;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import static java.util.Objects.*;

public class ElasticSearchIndexer {
	
	private final String indexName;
	
	public ElasticSearchIndexer(String indexName) {
		this.indexName = requireNonNull(indexName, "indexName cannot be null");
	}
	
	/**
	 * Maps the data set to Client DO object and save it to elastic cluster.
	 * 
	 * @param rows
	 */
	public void index(JavaRDD<CustomerDO> customerRDD) {
		JavaEsSpark.saveToEs(customerRDD, String.format("/%s", indexName));
	}



}
