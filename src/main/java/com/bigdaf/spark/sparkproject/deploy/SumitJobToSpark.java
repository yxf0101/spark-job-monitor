package com.bigdaf.spark.sparkproject.deploy;

import org.apache.spark.deploy.SparkSubmit;

public class SumitJobToSpark {
    public static void submitJob(String db,String table){
        String[] args = {
                "--class", "org.shadowmask.engine.spark.envirement.RuleBasedSparkMask",
                "--master", "spark://mot1:6066",
                "--deploy-mode", "cluster",
                "hdfs://192.168.1.90:9000/maskjar/sparkEngine-1.0-SNAPSHOT.jar",
                 db,table};

        SparkSubmit.main(args);


    }
}
