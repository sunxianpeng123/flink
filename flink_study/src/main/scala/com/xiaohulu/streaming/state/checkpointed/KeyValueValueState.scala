package com.xiaohulu.streaming.state.checkpointed

import java.net.URI

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.log4j.{Level, Logger}

/**
  * \* Created with IntelliJ IDEA.
  * \* User: sunxianpeng
  * \* Date: 2020/5/19
  * \* Time: 14:08
  * \* To change this template use File | Settings | File Templates.
  * \* Description:当接收到的相同 key 的元素个数等于 3 个或者超过 3 个的时候，就计算这些元素的 value 的平均值。计算 keyed stream 中每 3 个元素的 value 的平均值
  * \*/
object KeyValueValueState {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    //获取flink的运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //
    //    val fsPath = "file:///checkpoints//"
    val fsPath = "file:///F:\\PythonProjects/checkpoints//fs/"
    val rocksdbPath = "file:///F:\\PythonProjects/checkpoints//rocksdb/"


    //env.setStateBackend(new MemoryStateBackend())
    //false是否以同步的方式进行状态数据记录
    //env.setStateBackend(new FsStateBackend(fsPath, false))
    env.setStateBackend(new RocksDBStateBackend(rocksdbPath,false))
    val config = env.getCheckpointConfig
    //任务流取消和故障时会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
    //ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION，表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint处理。
    //ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION： 取消作业时保留检查点。请注意，在这种情况下，您必须在取消后手动清理检查点状态。
    //ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION： 取消作业时删除检查点。只有在作业失败时，检查点状态才可用。
    //默认情况下，如果设置了Checkpoint选项，则Flink只保留最近成功生成的1个Checkpoint，而当Flink程序失败时，可以从最近的这个Checkpoint来进行恢复。
    //但是，如果希望保留多个Checkpoint，并能够根据实际需要选择其中一个进行恢复，这样会更加灵活，比如，发现最近4个小时数据记录处理有问题，希望将整个状态还原到4小时之前。
    //Flink可以支持保留多个Checkpoint，需要在Flink的配置文件conf/flink-conf.yaml中，添加如下配置，指定最多需要保存Checkpoint的个数： state.checkpoints.num-retained: 20
    //Flink checkpoint目录分别对应的是 jobId，flink提供了在启动之时通过设置 -s 参数指定checkpoint目录, 让新的jobId 读取该checkpoint元文件信息和状态信息，从而达到指定时间节点启动job。
    config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    // 设置checkpoint的周期, 每隔1000 ms进行启动一个检查点
    config.setCheckpointInterval(1000)
    // 设置模式为exactly-once
    config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
    config.setMinPauseBetweenCheckpoints(500)
    // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
    config.setCheckpointTimeout(60000)
    // 同一时间只允许进行一个检查点
    config.setMaxConcurrentCheckpoints(1)


    val keyStream = env.fromElements((1L, 3L), (1L, 5L),
      (1L, 7L), (2L, 4L),
      (2L, 2L), (2L, 5L))
      .keyBy(0)
    //keyStream.print()

    val result = keyStream.flatMap(new CountWindowAverageWithValueState())
    result.print()
    //    输出结果
    //    6> (1,5.0)
    //    8> (2,3.6666666666666665)
    env.execute("ExampleManagedState")
  }
}

class CountWindowAverageWithValueState extends RichFlatMapFunction[(Long, Long), (Long, Double)] {
  // 用以保存每个 key 出现的次数，以及这个 key 对应的 value 的总值
  // managed keyed state
  //1. ValueState 保存的是对应的一个 key 的一个状态值
  private var countAndSum: ValueState[(Long, Long)] = _

  override def open(parameters: Configuration): Unit = {
    // 注册状态
    // 状态的名字 average
    // 状态存储的数据类型 createTypeInformation[(Long, Long)]
    val descriptor = new ValueStateDescriptor[(Long, Long)]("average", createTypeInformation[(Long, Long)])
    countAndSum = getRuntimeContext.getState(descriptor)
  }

  override def flatMap(input: (Long, Long), out: Collector[(Long, Double)]): Unit = {
    // 拿到当前的 key 的状态值
    var currentState = countAndSum.value
    // 如果状态值还没有初始化，则初始化
    if (currentState == null) currentState = (0L, 0L)
    // 更新状态值中的元素的个数, 更新状态值中的总值
    val count = currentState._1 + 1
    val sum = currentState._2 + input._2
    currentState = (count, sum)
    // 更新状态
    countAndSum.update(currentState)
    // 判断，如果当前的 key 出现了 3 次，则需要计算平均值，并且输出
    if (currentState._1 >= 3) {
      //      val keyStream = env.fromElements((1L, 3L), (1L, 5L),
      //      (1L, 7L), (2L, 4L),
      //      (2L, 2L), (2L, 5L))
      //此处除法会保留整数
      val avg = currentState._2.toDouble / currentState._1
      // 输出 key 及其对应的平均值
      out.collect((input._1, avg))
      //清空状态值
      countAndSum.clear()
    }
  }
}
