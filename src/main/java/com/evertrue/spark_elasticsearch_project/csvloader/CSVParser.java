package com.evertrue.spark_elasticsearch_project.csvloader;

import java.util.Objects;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;

public class CSVParser {
	private static final Logger LOGGER = Logger.getLogger(CSVParser.class.getName());

	private final String fileName;
	
	public CSVParser(String fileName) {
		this.fileName = Objects.requireNonNull(fileName, "fileName cannot be null");
	}
	
	public static JavaRDD<CustomerDO> parse(JavaRDD<String> stringJavaRDD ) {
		String header = stringJavaRDD.first();
		return stringJavaRDD
				.filter(obj -> !header.equals(obj))
				.map(line -> line.split("\n"))
				.map(RowFactory::create)
				.map(CSVParser::toCustomerDO);
		//rddOfRows = rddOfArrays.map(fields -> RowFactory.create(fields));
		
	}
	
	private static CustomerDO toCustomerDO(Row fields) {
		String customer = (String) fields.get(0);
		String[] clientDOObj = customer.split(",");
		return new CustomerDO(Integer.valueOf(clientDOObj[0]), clientDOObj[1], clientDOObj[2],
				Integer.valueOf(clientDOObj[3]), clientDOObj[4]);
	}

	/**
	 * This Method takes care of parsing the given CSV file to Resilient
	 * Distributed Data sets
	 * 
	 * @param args
	 * @return
	 */
	static JavaRDD<Row> parseCSVFileWithLocalESCluster(String... args) {
	
		SparkConf sparkConf = new SparkConf().setAppName("Evertrue CSV loader Demo Project");
		// .setMaster("local[*]") ; // Delete this line when running through
		// spark submit job
		JavaSparkContext sparkContext = null;
		JavaRDD<Row> rddOfRows = null;
		try {
			sparkContext = new JavaSparkContext(sparkConf);
			JavaRDD<String> stringJavaRDD = sparkContext.textFile(args[0]);
			String header = stringJavaRDD.first();
			JavaRDD<String[]> rddOfArrays = stringJavaRDD.filter(obj -> !header.equals(obj))
					.map(line -> line.split(";"));
			rddOfRows = rddOfArrays.map(fields -> RowFactory.create(fields));
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
		} finally {
			sparkContext.close();
		}
		return rddOfRows;
	}

	/**
	 * This Method takes Elastic search cluster hostname and port number as
	 * input and parse the given CSV file to Resilient Distributed Data sets
	 * 
	 * @param args
	 * @return
	 */
	static JavaRDD<CustomerDO> parseCSVFileWithRemoteESCluster(String... args) {
	
		JavaRDD<Row> rddOfRows =null;
		try {
			String fileName = args[0];
			String esNode = args[1];
			String esPort = args[2];
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

			//String header = stringJavaRDD.first();
			//JavaRDD<String[]> rddOfArrays = stringJavaRDD
											//.filter(obj -> !header.equals(obj))
			//								.map(line -> line.split("\n"));
			//rddOfRows = rddOfArrays.map(fields -> RowFactory.create(fields));
			return parse(stringJavaRDD);
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
		}
		return null;

	}
}
