package com.bigdaf.spark.sparkproject.deploy;

import org.apache.spark.SparkConf;
import org.apache.spark.deploy.rest.CreateSubmissionResponse;
import org.apache.spark.deploy.rest.RestSubmissionClient;
import org.apache.spark.deploy.rest.SubmissionStatusResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class SparkMonitorEngine {

    private static final Logger log = LoggerFactory.getLogger(SparkMonitorEngine.class);

    private static final String MASTER="spark://mot1:6066";
    private static final String APPNAME="RuleBasedSparkMask";

    public static String submit(String appResource,String mainClass,String ...args){
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster(MASTER);
        sparkConf.setAppName(APPNAME+" "+ System.currentTimeMillis());
       // sparkConf.set("spark.executor.cores","2");
        sparkConf.set("spark.submit.deployMode","cluster");
        sparkConf.set("spark.jars",appResource);
       // sparkConf.set("spark.executor.memory","2G");
       // sparkConf.set("spark.cores.max","2");
        sparkConf.set("spark.driver.supervise","false");


        Map<String,String> env = filterSystemEnvironment(System.getenv());

        CreateSubmissionResponse response = null;
        try {
            response = (CreateSubmissionResponse)
                    RestSubmissionClient.run(appResource, mainClass, args, sparkConf, toScalaMap(env));
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }

        return response.submissionId();
    }

    private static RestSubmissionClient client = new RestSubmissionClient(MASTER);
    public static String monitory(String appId){
        SubmissionStatusResponse response = null;
        boolean finished =false;
        String content="";
        int i =0;
        while(!finished) {
            try {
                response = (SubmissionStatusResponse) client.requestSubmissionStatus(appId, true);
                log.info("i:{}\t DriverState :{}",i++,response.driverState());
                if("FINISHED" .equals(response.driverState())){
                     content="任务完成";
                     finished = true;

                }else if("ERROR".equals(response.driverState())){
                    content="任务失败";
                    finished = true;
                }
                Thread.sleep(5000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
              log.info("Monitor done!");
               return content;

    }

    private static Map<String, String> filterSystemEnvironment(Map<String, String> env) {
        Map<String,String> map = new HashMap<>();
        for(Map.Entry<String,String> kv : env.entrySet()){
            if(kv.getKey().startsWith("SPARK_") && kv.getKey() != "SPARK_ENV_LOADED"
                    || kv.getKey().startsWith("MESOS_")){
                map.put(kv.getKey(),kv.getValue());
            }
        }
        return map;
    }

    /**
     * 这个方法实际返回的结果也是空，所以直接返回空即可
     * @param m
     * @param <A>
     * @param <B>
     * @return
     */
    public static <A, B> scala.collection.immutable.Map<A, B> toScalaMap(Map<A, B> m) {
//        return JavaConverters.mapAsScalaMapConverter(m).asScala().toMap(
//                Predef.<Tuple2<A, B>>conforms()
//        )
        return new  scala.collection.immutable.HashMap();
    }
}
