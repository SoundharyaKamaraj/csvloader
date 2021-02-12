package com.evertrue.spark_elasticsearch_project.csvloader;


/**
 * Customer Elastic search indexes CustomerDO to ES cluster.
 * @author Soundharya
 *
 */
public class CustomerESIndexer extends ElasticSearchIndexer<CustomerDO>{

	public CustomerESIndexer(String indexName) {
		super(indexName);
	}

}
