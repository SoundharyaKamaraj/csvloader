package com.evertrue.spark_elasticsearch_project.csvloader;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;


public class CSVParserUtil implements Serializable{
	
	private static final long serialVersionUID = 1L;
	
	/**
	 * Parse the given CSV and maps the custom object with the mapper function.
	 * @param sparkSession
	 * @param fileName
	 * @param mapper
	 * @return
	 */
	public static <T> JavaRDD<T> parse(SparkSession sparkSession, String fileName, Function<String[], T> mapper) {
		
		JavaRDD<String> stringJavaRDD = sparkSession.read()
										.textFile(fileName)
										.javaRDD();

		String header = stringJavaRDD.first();
		
		return stringJavaRDD
				.filter(obj -> !header.equals(obj))
				.map(RowFactory::create)
				.map(row -> (String) row.get(0))
				.map(row -> row.split(","))
				.map(mapper);
	}

}
