package org.zgg.gmall.realtime.app

import java.{lang, util}
import java.util.Date
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.zgg.gmall.realtime.bean.{DauInfo, PageLog}
import org.zgg.gmall.realtime.util.{MyBeanUtils, MyEsUtils, MyKafkaUtils, MyOffsetsUtils, MyRedisUtils}
import redis.clients.jedis.Jedis

import java.text.SimpleDateFormat
import java.time.{LocalDate, Period}
import scala.collection.mutable.ListBuffer

/**
 * 日活宽表
 *
 * 1. 准备实时环境
 * 2. 从Redis中读取偏移量
 * 3. 从kafka中消费数据
 * 4. 提取偏移量结束点
 * 5. 处理数据
 *     5.1 转换数据结构
 *     5.2 去重
 *     5.3 维度关联
 * 6. 写入ES
 * 7. 提交offsets
 */
object DwdDauApp {
  def main(args: Array[String]): Unit = {
    //1. 准备实时环境
    val sparkConf: SparkConf = new SparkConf().setAppName("dwd_dau_app").setMaster("local[2]")
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(5))

    //2. 从Redis中读取offset
    val topicName : String = "DWD_PAGE_LOG_TOPIC"
    val groupId : String = "DWD_DAU_GROUP"
    val offsets: Map[TopicPartition, Long] = MyOffsetsUtils.readOffset(topicName, groupId)

    //3. 从Kafka中消费数据
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if(offsets != null && offsets.nonEmpty){
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc,topicName,groupId,offsets)
    }else {
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc,topicName,groupId)
    }

    //4. 提取offset结束点
    var offsetRanges: Array[OffsetRange] = null
    val offsetRangesDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )
    //5. 处理数据
    // 5.1 转换结构
    val pageLogDStream: DStream[PageLog] = offsetRangesDStream.map(
      consumerRecord => {
        val value: String = consumerRecord.value()
        val pageLog: PageLog = JSON.parseObject(value, classOf[PageLog])
        pageLog
      }
    )
    // PageLog(mid_130,7,1,huawei,0,Xiaomi 9,Android 8.1,v2.1.111,Xiaomi,trade,cart,31,sku_ids,1728,null,1647779796000)
//    pageLogDStream.print(100)

    pageLogDStream.cache()
    pageLogDStream.foreachRDD(
      rdd => println("自我审查前: " + rdd.count())
    )
    // 5.2 去重
    // 5.2.1 自我审查: 将页面访问数据中last_page_id不为空的数据过滤掉
    val filterDStream:DStream[PageLog] = pageLogDStream.filter(
      pageLog => pageLog.last_page_id == null
    )

    filterDStream.cache()
    filterDStream.foreachRDD(
      rdd => {
        // 审查前后的数据，肯定是变少了
        println("自我审查后: " + rdd.count())
        println("----------------------------")
      }
    )

    // 5.2.2 第三方审查:  通过redis将当日活跃的mid维护起来,自我审查后的每条数据需要到redis中进行比对去重
    // redis中如何维护日活状态
    // 类型:    set
    // key :    DAU:DATE
    // value :  mid的集合
    // 写入API: sadd
    // 读取API: smembers
    // 过期:  24小时
    //filterDStream.filter()  // 每条数据执行一次. redis的连接太频繁.  【思考为什么使用mapPartitions】
    val redisFilterDStream: DStream[PageLog] = filterDStream.mapPartitions(
      pageLogIter => {
        val pageLogList: List[PageLog] = pageLogIter.toList
        println("第三方审查前: " + pageLogList.size)

        // 用来存储要的数据
        val pageLogs: ListBuffer[PageLog] = ListBuffer[PageLog]()
        val sdf: SimpleDateFormat = new SimpleDateFormat("yyy-MM-dd")
        val jedis: Jedis = MyRedisUtils.getJedisFromPool()
        for (pageLog <- pageLogList) {
          // 提取每条数据中的mid (我们日活的统计基于mid， 也可以基于uid)
          val mid: String = pageLog.mid
          //获取日期 , 因为我们要测试不同天的数据，所以不能直接获取系统时间.
          val ts: Long = pageLog.ts
          val date: Date = new Date(ts)
          val dateStr: String = sdf.format(date)
          val redisDauKey: String = s"DAU:$dateStr"

          //redis的判断是否包含操作
          /*
          下面代码在分布式环境中，存在并发问题， 可能多个并行度同时进入到if中,导致最终保留多条同一个mid的数据.
          // list
          val mids: util.List[String] = jedis.lrange(redisDauKey, 0 ,-1)
          if(!mids.contains(mid)){
            jedis.lpush(redisDauKey , mid )
            pageLogs.append(pageLog)
          }
          // set
          val setMids: util.Set[String] = jedis.smembers(redisDauKey)
          if(!setMids.contains(mid)){
            jedis.sadd(redisDauKey,mid)
            pageLogs.append(pageLog)
          }
          */
          // redis是单线程  判断包含和写入实现了原子操作
          val isNew: lang.Long = jedis.sadd(redisDauKey, mid)
          if (isNew == 1L) {
            pageLogs.append(pageLog)
          }
        }
        jedis.close()
        println("第三方审查后: " + pageLogs.size)
        pageLogs.iterator
      }
    )
//    redisFilterDStream.print()

    // 5.3 维度关联
    val dauInfoDStream: DStream[DauInfo] = redisFilterDStream.mapPartitions(
      pageLogIter => {
        val dauInfoes: ListBuffer[DauInfo] = ListBuffer[DauInfo]()
        val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val jedis: Jedis = MyRedisUtils.getJedisFromPool()
        for (pageLog <- pageLogIter){
          val dauInfo:DauInfo = new DauInfo()
          //1. 将pagelog中已有的字段拷贝到DauInfo中
          //笨办法: 将pageLog中的每个字段的值挨个提取，赋值给dauInfo中对应的字段。
          //dauInfo.mid = pageLog.mid
          //好办法: 通过对象拷贝来完成.
          MyBeanUtils.copyProperties(pageLog, dauInfo)

          //2. 补充维度
          //2.1  用户信息维度
          val uid: String = pageLog.user_id
          val redisUidkey: String = s"DIM:USER_INFO:$uid"
          val userInfoJson: String = jedis.get(redisUidkey)
          val userInfoJsonObj: JSONObject = JSON.parseObject(userInfoJson)
          //提取性别
          val gender: String = userInfoJsonObj.getString("gender")
          //提取生日  【思考为什么要保存生日，而不是直接保存年龄】
          val birthday: String = userInfoJsonObj.getString("birthday") // 1976-03-22
          //换算年龄
          val birthdayLd: LocalDate = LocalDate.parse(birthday)
          val nowLd: LocalDate = LocalDate.now()
          val period: Period = Period.between(birthdayLd, nowLd)
          val age: Int = period.getYears

          //补充到对象中
          dauInfo.user_gender = gender
          dauInfo.user_age = age.toString

          //2.2  地区信息维度
          // redis中:
          // 现在: DIM:BASE_PROVINCE:1
          // 之前: DIM:BASE_PROVINCE:110000
          val provinceID: String = dauInfo.province_id
          val redisProvinceKey: String = s"DIM:BASE_PROVINCE:$provinceID"
          val provinceJson: String = jedis.get(redisProvinceKey)
          val provinceJsonObj: JSONObject = JSON.parseObject(provinceJson)
          val provinceName: String = provinceJsonObj.getString("name")
          val provinceIsoCode: String = provinceJsonObj.getString("iso_code")
          val province3166: String = provinceJsonObj.getString("iso_3166_2")
          val provinceAreaCode: String = provinceJsonObj.getString("area_code")

          //补充到对象中
          dauInfo.province_name = provinceName
          dauInfo.province_iso_code = provinceIsoCode
          dauInfo.province_3166_2 = province3166
          dauInfo.province_area_code = provinceAreaCode

          //2.3  日期字段处理
          val date: Date = new Date(pageLog.ts)
          val dtHr: String = sdf.format(date)
          val dtHrArr: Array[String] = dtHr.split(" ")
          val dt: String = dtHrArr(0)
          val hr: String = dtHrArr(1).split(":")(0)
          //补充到对象中
          dauInfo.dt = dt
          dauInfo.hr = hr

          dauInfoes.append(dauInfo)
        }
        jedis.close()
        dauInfoes.iterator
      }
    )
    // DauInfo(mid_151,76,7,xiaomi,1,Huawei P30,Android 11.0,v2.1.134,Huawei,home,null,null,null,11662,M,29,江苏,CN-32,CN-JS,320000,2022-03-21,21,1647868844000)
//    dauInfoDStream.print(100)

    //写入到OLAP中
    //按照天分割索引，通过索引模板控制mapping、settings、aliases等.
    //准备ES工具类
    dauInfoDStream.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          dauInfoIter => {
            val docs: List[(String, DauInfo)] = dauInfoIter.map(dauInfo => (dauInfo.mid, dauInfo)).toList
            if (docs.size > 0){
              // 索引名
              // 如果是真实的实时环境，直接获取当前日期即可.
              // 因为我们是模拟数据，会生成不同天的数据.
              // 从第一条数据中获取日期
              val head: (String, DauInfo) = docs.head
              val ts: Long = head._2.ts
              val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
              val dateStr: String = sdf.format(new Date(ts))
              val indexName : String = s"gmall_dau_info_$dateStr"
              //写入到ES中
              MyEsUtils.bulkSave(indexName, docs)
            }
          }
        )
        //提交offset
        MyOffsetsUtils.saveOffset(topicName, groupId , offsetRanges)
      }
    )
    ssc.start()
    ssc.awaitTermination()
  }
}
