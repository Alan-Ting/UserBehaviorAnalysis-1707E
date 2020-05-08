import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @ author yulong
  * @ createTime 2020-04-10 11:44
  */
object LoginFailCEP {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //1. 定义输入数据流
    //    val loginEventStream = env.fromCollection(List(
    //      LoginEvent(1, "192.168.0.1", "fail", 1558430842),
    //      //      LoginEvent(1, "192.168.0.3", "success", 1558430845),
    //      LoginEvent(1, "192.168.0.2", "fail", 1558430843),
    //      LoginEvent(1, "192.168.0.3", "fail", 1558430844),
    //      LoginEvent(2, "192.168.10.10", "success", 1558430845)
    //    ))
    val loginEventStream = env.socketTextStream("hadoop102",7777)
      .map(data => {
        val dataArr = data.split(",")
        LoginEvent(dataArr(0).trim.toLong, dataArr(1).trim, dataArr(2).trim, dataArr(3).trim.toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(1)) {
        override def extractTimestamp(element: LoginEvent): Long = element.evenTime * 1000
      }).keyBy(_.userId)

    //2. 定义匹配模式
    val loginFailPattern: Pattern[LoginEvent, LoginEvent] = Pattern.begin[LoginEvent]("begin").where(_.eventType == "fail")
      .next("next").where(_.eventType == "fail")
      .within(Time.seconds(2))


    //3. 将pattern应用到输入流
    val patternStream: PatternStream[LoginEvent] = CEP.pattern(loginEventStream, loginFailPattern)

    //4. 用select检出符合模式的事件序列
    patternStream.select(new LoginFailMatch()).print("warning")

    env.execute("login fail with cep")

  }

}

//自定义PatternSelectFunction 输出报警
class LoginFailMatch() extends PatternSelectFunction[LoginEvent, Warning] {
  override def select(pattern: util.Map[String, util.List[LoginEvent]]): Warning = {

    //第一个失败事件
    val firstFail: LoginEvent = pattern.get("begin").iterator().next()
    //第二个失败事件
    val latestFail = pattern.get("next").iterator().next()
    //包装成warning输出信息
    Warning(firstFail.userId, firstFail.evenTime, latestFail.evenTime, "在2秒内连续2次登陆失败！")
  }
}
