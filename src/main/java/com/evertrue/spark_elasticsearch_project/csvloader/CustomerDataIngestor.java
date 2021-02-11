package com.evertrue.spark_elasticsearch_project.csvloader;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

/**
 * This class gets executed on submit of a spark job/ run as a stand alone
 * application Input - CSV file, Elastic Search hostname and Elastic Search port
 * number
 * 
 * 
 */
public class CustomerDataIngestor {
	
	private static final Logger LOGGER = Logger.getLogger(CustomerDataIngestor.class.getName());

	public static void main(String... args) {
		
		if (args.length == 0) {
			LOGGER.info("Please provide file in CSV format with Elastic search port and host");
			System.exit(0);
		}
		
		LOGGER.info("Starting the ETL process");
		String fileName = args[0];
		String esNode = args[1];
		String esPort = args[2];
		String indexName = args[3];
		
		//JavaRDD<Row> rddOfRows = CSVParser.parseCSVFileWithLocalESCluster(args);
		JavaRDD<CustomerDO> rddOfRows = CSVParser.parseCSVFileWithRemoteESCluster(args);
		
		new ElasticSearchIndexer(indexName).index(rddOfRows);
		
		LOGGER.info("Ending thee ETL job");

	}

	
	


}
