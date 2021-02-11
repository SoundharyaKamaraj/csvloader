package com.evertrue.spark_elasticsearch_project.csvloader;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

/**
 * This class gets executed on submit of a spark job/ run as a stand alone
 * application Input - CSV file
 * 
 * 
 */
public class CSVLoderImpl {
	static Logger log = Logger.getLogger(CSVLoderImpl.class.getName());

	public static void main(String... args) {
		if (args.length == 0) {
			log.info("Please provide file in CSV format");
			System.exit(0);
		}

		log.info("Starting the ETL for CSV:" + args[0]);
		JavaRDD<Row> rddOfRows = CSVParser.parseCSVFile(args);
		saveToElasticSearch(rddOfRows);
		log.info("Ending thee ETL job");

	}

	

	/**
	 * This Method takes care of mapping the data set to Client DO object and
	 * save it to elastic cluster
	 * 
	 * @param rddOfRows
	 */
	private static void saveToElasticSearch(JavaRDD<Row> rddOfRows) {
		JavaRDD<CustomerDO> customerDOObj = rddOfRows.map((Function<Row, CustomerDO>) fields -> {
			String objArray = (String) fields.get(0);
			String[] clientDOObj = objArray.split(",");
			return new CustomerDO(Integer.valueOf(clientDOObj[0]), clientDOObj[1], clientDOObj[2],
					Integer.valueOf(clientDOObj[3]), clientDOObj[4]);
		});

		JavaEsSpark.saveToEs(customerDOObj, "/customer");
	}

}
