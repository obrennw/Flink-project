package org.example

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */



import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig

/**
 * Skeleton for a Flink Job.
 *
 * You can also generate a .jar file that you can submit on your Flink
 * cluster. Just type
 * {{{
 *   sbt clean assembly
 * }}}
 * in the projects root directory. You will find the jar in
 * target/scala-2.11/Flink\ Project-assembly-0.1-SNAPSHOT.jar
 *
 */
object Job {
  def main(args: Array[String]) {
    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

		env.enableCheckpointing(100);
   
    val windowTime: Long = 1L

		val connectionConfig = new RMQConnectionConfig.Builder()
				.setHost("localhost")
				.setVirtualHost("/")
				.setUserName("guest")
				.setPassword("guest")
				.setPort(5672)
				.build();

		val stream = env
		    .addSource(new RMQSource[String](
						connectionConfig,            
						"flink",                 
						false,                        
						new SimpleStringSchema()))
				.setParallelism(1)
				
	  val average = stream
	       .map(s => (1,Integer.parseInt(s)))
				.timeWindowAll(Time.seconds(windowTime))
				.reduce((a,b) => (a._1+b._1,a._2+b._2))
				.map(t => (t._1,t._2/t._1).toString())
		
		average.print();
		average.addSink(new RMQSink[String](
                    connectionConfig,         // config for the RabbitMQ connection
                    "flink-out",              // name of the RabbitMQ queue to send messages to
                    new SimpleStringSchema)) // serialization schema to turn Java objects to messages
		
//  average.print();
//		stream.print();
    env.execute("Flink Scala API Skeleton")
//  RowReciever.exit()
  }
}
