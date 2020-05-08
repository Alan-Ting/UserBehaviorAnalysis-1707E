import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @ author yulong
  * @ createTime 2020-04-11 10:47
  * 1, "create", 1558430842
  * 2, "create", 1558430843
  * 2, "other", 1558430845
  * 2, "pay", 1558430850
  * 1, "pay", 1558431920
  */
//定义输入样例类
case class OrderEvent(orderId:Long, eventType:String, eventTime:Long)
//输出检测结果的样例类
case class OrderResult(orderId:Long, resultMsg:String)

object OrderTimeOut {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //1. 订单数据
//    val orderEventStream = env.fromCollection(List(
//      OrderEvent(1, "create", 1558430842),
//      OrderEvent(2, "create", 1558430843),
//      OrderEvent(2, "other", 1558430845),
//      OrderEvent(2, "pay", 1558430850),
//      OrderEvent(1, "pay", 1558431920)
//    ))
val orderEventStream = env.socketTextStream("hadoop102",7777)
  .map(data =>{
    val dataArr = data.split(",")
    OrderEvent(dataArr(0).trim.toLong, dataArr(1).trim, dataArr(2).trim.toLong)
  })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(1)) {
      override def extractTimestamp(element: OrderEvent): Long = element.eventTime * 1000
    })
      .keyBy(_.orderId)

    //2. 定义匹配模式
    val orderPayPattern = Pattern.begin[OrderEvent]("start").where(_.eventType == "create")
      .followedBy("follow").where(_.eventType == "pay")
      .within(Time.minutes(10))

    //3. 将匹配规则应用到数据流
    val patternStream = CEP.pattern(orderEventStream, orderPayPattern)

    //4. 定义侧输出流标签
    val orderTimeOutputTage = new OutputTag[OrderResult]("orderTimeout")

    //5. 从pattern中提取事件序列
    val complexResultStream = patternStream.select(orderTimeOutputTage, new OrderTimeoutSelect(), new OrderPaySelect())

    //6. 分别从主流和侧输出流打印数据
    complexResultStream.print("pay order")
    complexResultStream.getSideOutput(orderTimeOutputTage).print("timeout order")

    env.execute("order timeout job")

  }

}

//处理检测到的超时序列
class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent, OrderResult]{
  override def timeout(pattern: util.Map[String, util.List[OrderEvent]], timeoutTimestamp: Long): OrderResult = {
    val timeoutOrderId = pattern.get("start").iterator().next().orderId
    OrderResult(timeoutOrderId, "timeout!")
  }
}

//处理成功匹配的事件序列
class OrderPaySelect() extends PatternSelectFunction[OrderEvent, OrderResult]{
  override def select(pattern: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val payOrderId = pattern.get("follow").iterator().next().orderId
    OrderResult(payOrderId, "pay successfully!")
  }
}