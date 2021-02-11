package com.evertrue.spark_elasticsearch_project.csvloader;

import java.util.Objects;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;

public class CSVParser {
	private static final Logger LOGGER = Logger.getLogger(CSVParser.class.getName());

	private final String fileName;
	private final String esNode;
	private final String esPort;
	
	public CSVParser(String fileName,String esNode, String esPort) {
		this.fileName = Objects.requireNonNull(fileName, "fileName cannot be null");
		this.esNode = Objects.requireNonNull(esNode, "Elastic search cluster hostname cannot be null");
		this.esPort = Objects.requireNonNull(esPort, "Elastic search cluster port cannot be null");
	}
	
	/**
	 * @param stringJavaRDD
	 * @return
	 */
	private static JavaRDD<CustomerDO> parse(JavaRDD<String> stringJavaRDD ) {
		String header = stringJavaRDD.first();
		return stringJavaRDD
				.filter(obj -> !header.equals(obj))
				.map(RowFactory::create)
				.map(CSVParser::toCustomerDO);
		
	}
	
	private static CustomerDO toCustomerDO(Row fields) {
		String customer = (String) fields.get(0);
		String[] clientDOObj = customer.split(",");
		return new CustomerDO(Integer.valueOf(clientDOObj[0]), clientDOObj[1], clientDOObj[2],
				Integer.valueOf(clientDOObj[3]), clientDOObj[4]);
	}

	/**
	 * This Method takes Elastic search cluster hostname and port number as
	 * input and parse the given CSV file to Resilient Distributed Data sets
	 * 
	 * @param args
	 * @return
	 */
	public  JavaRDD<CustomerDO> parseCSVFileWithRemoteESCluster(String... args) {
	
		JavaRDD<CustomerDO> rddOfRows =null;
		try {
			
			SparkSession sparkConf = SparkSession.builder()
									.config("spark.es.nodes", esNode)
									.config("spark.es.port", esPort)
									.config("es.index.auto.create", "true")
									.config("spark.es.nodes.wan.only", "true")
									.config("es.batch.size.entries", "1")
									.appName("Evertrue CSV loader Demo Project")
									.getOrCreate();
			
			JavaRDD<String> stringJavaRDD = sparkConf
											.read()
											.textFile(fileName)
											.javaRDD();

			rddOfRows = parse(stringJavaRDD);
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
		}
		return rddOfRows;

	}
}
