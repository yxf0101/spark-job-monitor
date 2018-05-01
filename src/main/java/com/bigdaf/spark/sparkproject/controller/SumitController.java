package com.bigdaf.spark.sparkproject.controller;


import com.bigdaf.spark.sparkproject.deploy.SparkMonitorEngine;
import com.bigdaf.spark.sparkproject.deploy.SumitJobToSpark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;



@Controller
@RequestMapping("/find")
public class SumitController {
    private static final Logger logger = LoggerFactory.getLogger(SumitController.class);
    @RequestMapping(value = "/mask",method = RequestMethod.GET)
    @ResponseBody
    public String sparkSubmit(@RequestParam(required = false) String db, @RequestParam(required = false) String table){
          logger.info("start sumbit spark job");
        /*  String dbName=db;
          String tableName=table;
        SumitJobToSpark.submitJob(dbName,tableName);*/
        String className ="org.shadowmask.engine.spark.envirement.RuleBasedSparkMask";
        String appName = "RuleBasedSparkMask";
        String dbName=db;
        String tableName=table;
        String jarPath="hdfs://mot1:9000/maskjar/sparkEngine-1.0-SNAPSHOT.jar";

        String id=SparkMonitorEngine.submit(jarPath,className,dbName,tableName);

        logger.info("id:"+id);

        Boolean flag=true;
        String contents="";
        while(flag){
            String infos=SparkMonitorEngine.monitory(id);
            if("".equals(infos) && null==infos){
                flag=true;
            }else{
                String content=infos;
                contents=content;
                flag=false;
            }


        }

            return contents;




    }
}
