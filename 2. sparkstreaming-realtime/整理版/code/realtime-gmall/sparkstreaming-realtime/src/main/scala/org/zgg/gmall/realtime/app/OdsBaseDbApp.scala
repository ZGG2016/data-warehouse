package org.zgg.gmall.realtime.app

import java.util
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.zgg.gmall.realtime.util.{MyEsUtils, MyKafkaUtils, MyOffsetsUtils, MyRedisUtils}
import redis.clients.jedis.{Jedis, Pipeline}

import java.time.LocalDate

/**
 * 业务数据消费分流
 *
 * 1. 准备实时环境
 *
 * 2. 从redis中读取偏移量
 *
 * 3. 从kafka中消费数据
 *
 * 4. 提取偏移量结束点
 *
 * 5. 数据处理
 *     5.1 转换数据结构
 *     5.2 分流
 *         事实数据 => Kafka
 *         维度数据 => Redis
 * 6. flush Kafka的缓冲区
 *
 * 7. 提交offset
 */
object OdsBaseDbApp {
  def main(args: Array[String]): Unit = {

    //0.还原状态
    revertState()

    // 1. 准备实时环境
    val sparkConf: SparkConf = new SparkConf().setAppName("ods_base_db_app").setMaster("local[2]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))
    // 注意：在测试的时候，使用的topic是ODS_BASE_DB_M
    val topicName: String = "ODS_BASE_DB"
    val groupId: String = "ODS_BASE_DB_GROUP"

    // 2. 从redis中读取偏移量
    val offsets: Map[TopicPartition, Long] = MyOffsetsUtils.readOffset(topicName, groupId)

    // 3. 从Kafka中消费数据
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if(offsets != null && offsets.nonEmpty){
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId, offsets)
    }else{
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId)
    }

    // 4. 提取偏移量结束点
    var offsetRanges: Array[OffsetRange] = null
    val offsetRangesDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )
    //5. 处理数据
    // 5.1 转换数据结构
    val jsonObjDStream: DStream[JSONObject] = offsetRangesDStream.map(
      consumerRecord => {
        val dataJson: String = consumerRecord.value()
        val jSONObject: JSONObject = JSON.parseObject(dataJson)
        jSONObject
      }
    )
    // {"database":"gmall","xid":6760,"data":{"using_time":"2022-03-22 02:05:01","coupon_id":2,"user_id":14,"get_time":"2022-03-22 02:05:01",
    // "used_time":"2022-03-22 02:05:02","id":918,"order_id":48,"coupon_status":"1403"},"xoffset":996,"type":"delete","table":"coupon_use","ts":1670033476}
//    jsonObjDStream.print(1000)

    //5.2 分流
    // 考虑Redis连接写到哪里
    // 【foreachRDD外面 ；foreachRDD里面, foreachPartition外面；foreachPartition里面 , 循环外面；foreachPartition里面,循环里面】
    jsonObjDStream.foreachRDD(
      rdd => {
        //如何动态配置表清单???  【考虑Redis连接的位置】
        // 将表清单维护到redis中，实时任务中动态的到redis中获取表清单.  【随着清单的修改，不用停掉程序】
        // 类型: set
        // key:  FACT:TABLES   DIM:TABLES
        // value : 表名的集合
        // 写入API: sadd
        // 读取API: smembers
        // 过期: 不过期
        val redisFactKeys: String = "FACT:TABLES"
        val redisDimKeys: String = "DIM:TABLES"
        val jedis: Jedis = MyRedisUtils.getJedisFromPool()
        //事实表清单
        val factTables :util.Set[String] = jedis.smembers(redisFactKeys)
        val factTablesBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(factTables)
        println("factTables: " + factTables)
        //维度表清单
        val dimTables: util.Set[String] = jedis.smembers(redisDimKeys)
        val dimTablesBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dimTables)
        println("dimTables: " + dimTables)
        jedis.close()

        rdd.foreachPartition(
          jsonObjIter => {
            //【考虑Redis连接的位置】
            val jedis: Jedis = MyRedisUtils.getJedisFromPool()
            for (jsonObj <- jsonObjIter){
              val opType: String = jsonObj.getString("type")
              val opValue: String = opType match {
                // 历史维度数据引导
                case "bootstrap-insert" => "BI"
                case "insert" => "I"
                case "update" => "U"
                case "delete" => "D"
                case _ => null
              }
              //判断操作类型: 1. 明确什么操作  2. 过滤不感兴趣的数据
              if (opValue != null){
                val tableName: String = jsonObj.getString("table")

                //事实数据
                if (factTablesBC.value.contains(tableName)){
                  val data: String = jsonObj.getString("data")
                  val dwdTopicName : String = s"DWD_${tableName.toUpperCase}_${opValue}"
                  MyKafkaUtils.send(dwdTopicName, data)

                  //模拟数据延迟  （用来测试数据延迟）
                  if(tableName.equals("order_detail")){
                    Thread.sleep(200)
                  }
                }
                //维度数据
                if (dimTablesBC.value.contains(tableName)){
                  //维度数据
                  // 类型 : string  hash
                  //        hash ： 整个表存成一个hash。 要考虑目前数据量大小和将来数据量增长问题 及 高频访问问题.
                  //        hash :  一条数据存成一个hash.
                  //        String : 一条数据存成一个jsonString.
                  // key :  DIM:表名:ID
                  // value : 整条数据的jsonString
                  // 写入API: set
                  // 读取API: get
                  // 过期:  不过期
                  //提取数据中的id
                  val dataObj: JSONObject = jsonObj.getJSONObject("data")
                  val id: String = dataObj.getString("id")
                  val redisKey : String = s"DIM:${tableName.toUpperCase}:$id"
                  jedis.set(redisKey, dataObj.toJSONString)
                }
              }
            }
            jedis.close()
            MyKafkaUtils.flush()
          }
        )
        MyOffsetsUtils.saveOffset(topicName, groupId, offsetRanges)
      }
    )

    ssc.start()
    ssc.awaitTermination()
   }
  /**
   * 状态还原
   *
   * 在每次启动实时任务时， 进行一次状态还原。 以ES为准, 将所以的mid提取出来，覆盖到Redis中.
   */

  def revertState(): Unit ={
    //从ES中查询到所有的mid
    val date: LocalDate = LocalDate.now()
    // 【考虑从哪个索引取数据：应该从当天生成的索引取mid】
    val indexName : String = s"gmall_dau_info_$date"
    val fieldName : String = "mid"
    val mids: List[ String ] = MyEsUtils.searchField(indexName , fieldName)
    //删除redis中记录的状态（所有的mid）
    val jedis: Jedis = MyRedisUtils.getJedisFromPool()
    val redisDauKey : String = s"DAU:$date"
    jedis.del(redisDauKey)
    //将从ES中查询到的mid覆盖到Redis中
    if(mids != null && mids.size > 0 ){
      /*for (mid <- mids) {
        jedis.sadd(redisDauKey , mid )
      }*/
      val pipeline: Pipeline = jedis.pipelined()
      for (mid <- mids) {
        pipeline.sadd(redisDauKey , mid )  //不会直接到redis执行
      }

      pipeline.sync()  // 到redis执行
    }
    jedis.close()
  }
}
