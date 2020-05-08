
import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * @ author yulong
  * @ createTime 2020-04-07 10:39
  */
//输入数据样例类
case class UserBehavior(userId:Long, itemId:Long, categoryId:Int, behavior:String, timeStamp:Long)
//中间输出的商品浏览量的样例类
case class ItemViewCount(itemId:Long, windowEnd:Long, count:Long)

object HotItems {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //设置时间语义为EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

//    val stream: DataStream[String] = env.readTextFile("D:\\Mywork\\workspace\\IdeaProjects\\baway2019\\UserBehaviorAnalysis-1707E\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")

    val properties = new Properties()
    properties.setProperty("bootstrap.servers","hadoop102:9092")
    properties.setProperty("group.id","test")

    val stream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("hotitemscount", new SimpleStringSchema(), properties))


    stream.map(data => {
      val dataArr: Array[String] = data.split(",")
      UserBehavior(dataArr(0).trim.toLong, dataArr(1).trim.toLong, dataArr(2).trim.toInt, dataArr(3).trim, dataArr(4).trim.toLong)
    }).assignAscendingTimestamps(_.timeStamp * 1000)
        .filter(_.behavior == "pv")
        .keyBy(_.itemId)
        .timeWindow(Time.hours(1), Time.minutes(5))
        .aggregate(new CountAgg(), new WindowResult())
        .keyBy(_.windowEnd)
        .process(new TopNHotItems(3))
        .print()

    env.execute("hot items")

  }

}

//自定义预聚合函数，来一条数据我就加1
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long]{
  override def createAccumulator(): Long = 0L

  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

//自定义窗口函数，包装成ItemViewCount输出
class WindowResult() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow]{
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId = key
    val windowEnd = window.getEnd
    val count = input.iterator.next()
    out.collect(ItemViewCount(itemId, windowEnd, count))
  }
}

//自定义process function，排序数据
class TopNHotItems(n: Int) extends KeyedProcessFunction[Long, ItemViewCount, String]{

  //定义状态list state，用来保存所有的ItemViewCount
  private var itemState:ListState[ItemViewCount] = _

  //对初始状态做声明
  override def open(parameters: Configuration): Unit = {
     itemState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemState",classOf[ItemViewCount]))
  }

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    //每一条数据都存到state里
    itemState.add(value)
    //注册定时器,定时器出发的时候，当前windowEnd的数据都到了，做排序处理
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 100)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

    //收集到所有数据，首先放到1个list里
    val allItems: ListBuffer[ItemViewCount] = new ListBuffer()

    //使用itemState.get()的foreach方法，引入JavaConversions
    import scala.collection.JavaConversions._
    for (item <- itemState.get()){
      allItems +=item
    }

    //清除状态，释放空间
    itemState.clear()

    //按照ItemViewCount大小排序
    val sortedItems: ListBuffer[ItemViewCount] = allItems.sortBy(_.count)(Ordering.Long.reverse).take(n)

    //将排好序的数据打印输出
    val result: StringBuilder = new StringBuilder()

    //设置分割线
    result.append("==============================\n")
    //当前的时间窗口
    result.append("时间： ").append(new Timestamp(timestamp - 100)).append("\n")
    //通过for循环输出商品信息
    for (elem <- sortedItems.indices) {
      val currentItem = sortedItems(elem)
      result.append("No").append(elem + 1).append(": ")
        .append("商品ID=").append(currentItem.itemId)
        .append("浏览量=").append(currentItem.count)
        .append("\n")
    }
    result.append("==============================\n")

    Thread.sleep(1000)

    out.collect(result.toString())

  }
}

