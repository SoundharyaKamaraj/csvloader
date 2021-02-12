package com.evertrue.spark_elasticsearch_project.csvloader;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;

/**
 * This class gets executed on submit of a spark job.
 * 
 * @author Soundharya
 * 
 */
public class CustomerDataIngestor{
	
	private static final Logger LOGGER = Logger.getLogger(CustomerDataIngestor.class.getName());
	
	public static final Function<String[], CustomerDO> CSV_MAPPER = fields -> new CustomerDO(Integer.valueOf(fields[0]), 
			fields[1], 
			fields[2], 
			Integer.valueOf(fields[3]), 
			fields[4]);
	
	public static void main(String... args) {
		
		if (args.length != 4) {
			LOGGER.info("Usage:\n"
					+ "./spark-submit --class com.evertrue.spark_elasticsearch_project.csvloader.CustomerDataIngestor evertrue_csvloader-jar-with-dependencies.jar "
					+ "--master spark://<MASTER_SPARK_HOST>:7077  "
					+ "<INPUT_FILE.csv> "
					+ "<ES_HOST> "
					+ "<ES_PORT> "
					+ "<ES_INDEX_NAME>");
			System.exit(0);
		}
		String fileName = args[0];
		String esNode = args[1];
		String esPort = args[2];
		String indexName = args[3];
		
		SparkSession sparkSession = SparkSession.builder()
								.config("spark.es.nodes", esNode)
								.config("spark.es.port", esPort)
								.config("es.index.auto.create", "true")
								.config("spark.es.nodes.wan.only", "true")
								.config("es.batch.size.entries", "1")
								.appName("Evertrue CSV loader Demo Project")
								.getOrCreate();
		
		LOGGER.info("Starting the ETL job");
		
		JavaRDD<CustomerDO> rddOfRows = CSVParserUtil.parse(sparkSession, fileName, CSV_MAPPER);
		
		ElasticSearchIndexer<CustomerDO> customerIndexer = new CustomerESIndexer(indexName);
		customerIndexer.index(rddOfRows);
		
		LOGGER.info("Ending the ETL job");

	}
}
