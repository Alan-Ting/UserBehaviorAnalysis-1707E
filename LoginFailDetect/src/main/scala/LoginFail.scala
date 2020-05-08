import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * @ author yulong
  * @ createTime 2020-04-10 10:25
  * 1, "192.168.0.1", "fail", 1558430842
  * 1, "192.168.0.2", "fail", 1558430843
  * 1, "192.168.0.3", "fail", 1558430844
  * 2, "192.168.10.10", "success", 1558430845
  */
//输入登录事件样例类
case class LoginEvent(userId: Long, ip: String, eventType: String, evenTime: Long)

//输出报警信息样例类
case class Warning(userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String)

object LoginFail {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val loginEventStream = env.fromCollection(List(
      LoginEvent(1, "192.168.0.1", "fail", 1558430842),
      LoginEvent(1, "192.168.0.2", "fail", 1558430843),
      LoginEvent(1, "192.168.0.3", "fail", 1558430844),
      LoginEvent(2, "192.168.10.10", "success", 1558430845)
    )).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(1)) {
      override def extractTimestamp(element: LoginEvent): Long = element.evenTime * 1000
    }).keyBy(_.userId)
      .process(new LoginWarning())
      .print()


    env.execute("login fail job")

  }

}

// 自定义处理函数，保存上一次登录失败的事件，并可以注册定时器
class LoginWarning() extends KeyedProcessFunction[Long, LoginEvent, Warning] {
  //定义保存登录失败的状态
  lazy val loginFailState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginfailstate", classOf[LoginEvent]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {

    // 判断当前登录状态是否为fail，如果是fail就存起来
    if (value.eventType == "fail") {
      loginFailState.add(value)
      //注册定时器
      ctx.timerService().registerEventTimeTimer((value.evenTime + 2) * 1000L)
    } else {
      //如果登陆成功，清空状态重新开始
      loginFailState.clear()
    }

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {

    //先把状态中的数据取出
    val allLoginFailEvents: ListBuffer[LoginEvent] = new ListBuffer[LoginEvent]()

    val iter = loginFailState.get().iterator()
    while (iter.hasNext) {
      allLoginFailEvents += iter.next()
    }

    //判断登陆失败事件个数，如果大于2，输出报警信息
    if (allLoginFailEvents.length >= 2) {
      out.collect(Warning(
        allLoginFailEvents.head.userId,
        allLoginFailEvents.head.evenTime,
        allLoginFailEvents.last.evenTime,
        "2秒内连续登陆失败" + allLoginFailEvents.length + "次"
      ))
    }

    //清空状态
    loginFailState.clear()

  }
}