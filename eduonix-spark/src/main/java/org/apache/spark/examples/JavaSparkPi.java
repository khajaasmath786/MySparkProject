/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.List;

/** 
 * Computes an approximation to pi
 * Usage: JavaSparkPi [slices]
 * https://github.com/apache/spark/blob/master/pom.xml
 * 
 * java -cp eduonix_spark-deploy.jar  org.apache.spark.examples.JavaSparkPi
 * 
 * 
 */
public final class JavaSparkPi {

  static boolean runOnCluster = false;

  public static void main(String[] args) throws Exception {
	  
	//System.setProperty("hadoop.home.dir", "c:\\winutil\\");
    SparkConf sparkConf = new SparkConf().setAppName("JavaSparkPi");
    int slices = 0;
    JavaSparkContext jsc = null;
    if(!runOnCluster) {
      sparkConf.setMaster("local[2]");
      // for intellij 
     // sparkConf.setJars(new String[]{"target/eduonix_spark-deploy.jar"});
      // for eclipse
     // sparkConf.setJars(new String[]{"eduonix_spark-deploy.jar"});
      
      
      slices = 10;
      jsc = new JavaSparkContext(sparkConf);
    } else {
      slices = (args.length == 1) ? Integer.parseInt(args[0]) : 2;
      jsc = new JavaSparkContext(sparkConf);
    }

    int n = 100000 * slices;
    List<Integer> l = new ArrayList<Integer>(n);
    for (int i = 0; i < n; i++) {
      l.add(i);
    }
    
    System.out.println("got this far " );

    JavaRDD<Integer> dataSet = jsc.parallelize(l, slices);

    int count = dataSet.map(new Function<Integer, Integer>() {
      @Override
      public Integer call(Integer integer) {
        double x = Math.random() * 2 - 1;
        double y = Math.random() * 2 - 1;
        return (x * x + y * y < 1) ? 1 : 0;
      }
    }).reduce(new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer integer, Integer integer2) {
        return integer + integer2;
      }
    });

    System.out.println("Pi is roughly " + 4.0 * count / n);

    jsc.stop();
  }
}
