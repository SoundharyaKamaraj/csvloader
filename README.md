# csvloader

Eclipse Workspace Setup

1) Import as maven project 
2) Do Maven clean install
3) Build should be successful and evertrue_csvloader-jar-with-dependencies.jar should be generated

Run Elastic Search on 
http://localhost:9200/

Run Kibana on 
http://localhost:5601/app/management/data/index_management/indices

Run spark on
http://localhost:8080/
Start Master cluster Eg: spark://Soundharyas-MacBook-Pro.local:7077
start slave cluster  Eg: ./sbin/start-slave.sh spark://Soundharyas-MacBook-Pro.local:7077

Job can be executed using below 2 ways 
1) Submitting the job through Spark-submit:
./spark-2.4.7-bin-hadoop2.7/bin/spark-submit --class com.evertrue.spark_elasticsearch_project.csvloader.CSVLoderImpl 
--master spark://Soundharyas-MacBook-Pro.local:7077  
/Users/Soundharya/eclipse/workspace/csvReader_evertrue/csvloader/target/evertrue_csvloader-jar-with-dependencies.jar 
/Users/Soundharya/eclipse/Workspace/csvReader_evertrue/csvloader/data.csv

2) Running the job through eclipse
Run configuration with arguments as the csv file name Eg: data.csv

Please refer csvloader/QueryResults.txt for the output generated executing below curl command 

GET customer/_search?pretty
{
  "aggs": {
    "averageByStateId": {
      "terms": {
        "field": "state.keyword",
        "size": 50
      },
      "aggs": {
        "average": {
          "avg": {
            "field": "age"
          }
        }
      }
    }
  }
}
