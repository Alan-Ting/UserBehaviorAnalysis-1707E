import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * @ author yulong
  * @ createTime 2020-04-09 10:44
  */

//输入数据样例类
case class LogEvent(ip:String, userId:String, eventTime:Long, method:String, url:String)
//中间结果样例类
case class UrlViewCount(url:String, windowEnd:Long, count:Long)

object NetworkFlow {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream: DataStream[String] = env.readTextFile("D:\\Mywork\\workspace\\IdeaProjects\\baway2019\\UserBehaviorAnalysis-1707E\\NetworkFlowAnalysis\\src\\main\\resources\\apache.log")

    stream.map(data =>{
      val dataArr: Array[String] = data.split(" ")
      val timeStamp: Long = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss").parse(dataArr(3).trim).getTime
      LogEvent(dataArr(0).trim, dataArr(1).trim, timeStamp, dataArr(5).trim, dataArr(6).trim)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LogEvent](Time.seconds(60)) {
      override def extractTimestamp(element: LogEvent): Long = element.eventTime
    })
      .keyBy(_.url)
      .timeWindow(Time.minutes(1),Time.seconds(5))
      .aggregate(new CountAgg(), new WindowResult())
      .keyBy(_.windowEnd)
      .process(new TopNUrls(5))
      .print()

    env.execute()


  }

}

//自定义预聚合函数
class CountAgg() extends AggregateFunction[LogEvent, Long, Long]{
  override def createAccumulator(): Long = 0L

  override def add(value: LogEvent, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

//自定义窗口处理函数IN, OUT, KEY, W <: Window
class WindowResult() extends WindowFunction[Long,UrlViewCount, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount(key,window.getEnd,input.iterator.next()))
  }
}

class TopNUrls(n:Int) extends KeyedProcessFunction[Long, UrlViewCount,String]{

  //定义一个状态listState，用来保存所有的UrlViewCount
  private var urlState : ListState[UrlViewCount] = _

  //对初始状态做个声明
  override def open(parameters: Configuration): Unit = {
    urlState = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("urlState",classOf[UrlViewCount]))
  }


  override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {

    //每条数据都存到state
    urlState.add(value)
    //注册定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

    val allUrlView: ListBuffer[UrlViewCount] = new ListBuffer[UrlViewCount]()
    val iter = urlState.get().iterator()

    while (iter.hasNext){
      allUrlView += iter.next()
    }

    urlState.clear()

    //基于count大小进行排序
    val sortedUrlView = allUrlView.sortWith(_.count > _.count).take(5)
    //格式化成string输出
    val result = new StringBuilder

    result.append("=============================\n")
    result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")

    for (elem <- sortedUrlView.indices) {

      val currentUrlView : UrlViewCount = sortedUrlView(elem)
      result.append("No").append(elem + 1).append(":")
        .append("URL=").append(currentUrlView.url)
        .append("流量=").append(currentUrlView.count).append("\n")
    }
    result.append("=============================\n")
    Thread.sleep(1000)

    out.collect(result.toString())

  }
}