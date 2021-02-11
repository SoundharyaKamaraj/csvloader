package com.evertrue.spark_elasticsearch_project.csvloader;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

public class CSVParser {

	public CSVParser() {
		// TODO Auto-generated constructor stub
	}
	
	/**
	 * This Method takes care of parsing the given CSV file to Resilient
	 * Distributed Data sets
	 * 
	 * @param args
	 * @return
	 */
	 static JavaRDD<Row> parseCSVFile(String... args) {
		
		SparkConf sparkConf = new SparkConf().setAppName("Evertrue CSV loader Demo Project");
		//.setMaster("local[*]") ; // Delete this line when submitting to a
		// cluster
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		JavaRDD<String> stringJavaRDD = sparkContext.textFile(args[0]);
		String header = stringJavaRDD.first();
		JavaRDD<String[]> rddOfArrays = stringJavaRDD.filter(obj -> !header.equals(obj)).map(line -> line.split(";"));
		JavaRDD<Row> rddOfRows = rddOfArrays.map(fields -> RowFactory.create(fields));
		return rddOfRows;
	}

}
