package com.xiaohulu.streaming.state.operatorstate

import org.apache.flink.api.common.state.{MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

object BroadcastStateFunction {

  /** main() defines and executes the DataStream program */
  def main(args: Array[String]) {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // checkpoint every 10 seconds
    env.getCheckpointConfig.setCheckpointInterval(10 * 1000)

    // use event time for the application
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // configure watermark interval
    env.getConfig.setAutoWatermarkInterval(1000L)

    // ingest sensor stream
    val sensorData: DataStream[SensorReading] = env
      // SensorSource generates random temperature readings
      .addSource(new SensorSource)
      // assign timestamps and watermarks which are required for event time
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)

    // define a stream of thresholds
    val thresholds: DataStream[ThresholdUpdate] = env.fromElements(
      ThresholdUpdate("sensor_1", 5.0d),
      ThresholdUpdate("sensor_2", 0.9d),
      ThresholdUpdate("sensor_3", 0.5d),
      ThresholdUpdate("sensor_1", 1.2d), // update threshold for sensor_1
      ThresholdUpdate("sensor_3", 0.0d)) // disable threshold for sensor_3

    val keyedSensorData: KeyedStream[SensorReading, String] = sensorData.keyBy(_.id)

    val broadcastStateDescriptor = new MapStateDescriptor[String, Double]("thresholds", classOf[String], classOf[Double])
    val broadcastThresholds: BroadcastStream[ThresholdUpdate] = thresholds.broadcast(broadcastStateDescriptor)

    val alerts: DataStream[(String, Double, Double)] = keyedSensorData
      .connect(broadcastThresholds) // 并关联广播状态数据
      .process(new UpdatableTemperatureAlertFunction())

    // print result stream to standard out
    alerts.print()

    // execute application
    env.execute("Generate Temperature Alerts")
  }
}

case class ThresholdUpdate(id: String, threshold: Double)

/**
  * The function emits an alert if the temperature measurement of a sensor changed by more than
  * a threshold compared to the last reading. The thresholds are configured per sensor by a separate stream.
  */
class UpdatableTemperatureAlertFunction() extends KeyedBroadcastProcessFunction[String, SensorReading, ThresholdUpdate, (String, Double, Double)] {
  //  读取广播状态，广播状态描述符
  private lazy val thresholdStateDescriptor = new MapStateDescriptor[String, Double]("thresholds", classOf[String], classOf[Double])

  // the state handle object,键值分区状态引用对象
  private var lastTempState: ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    // create state descriptor
    val lastTempDescriptor = new ValueStateDescriptor[Double]("lastTemp", classOf[Double])
    // obtain the state handle获取键值分区状态引用对象
    lastTempState = getRuntimeContext.getState[Double](lastTempDescriptor)
  }

  //获取广播更新变量,广播中有新增流数据才会执行，根据并行度次数执行
  override def processBroadcastElement(
                                        update: ThresholdUpdate,
                                        ctx: KeyedBroadcastProcessFunction[String, SensorReading, ThresholdUpdate, (String, Double, Double)]#Context,
                                        out: Collector[(String, Double, Double)]): Unit = {
    //获取广播状态引用对象
    val thresholds = ctx.getBroadcastState(thresholdStateDescriptor)

    if (update.threshold != 0.0d) {
      // configure a new threshold for the sensor，为指定传感器配置新的阈值
      thresholds.put(update.id, update.threshold)
    } else {
      // remove threshold for the sensor，删除该传感器的阈值
      thresholds.remove(update.id)
    }
  }

  //
  override def processElement(
                               reading: SensorReading,
                               readOnlyCtx: KeyedBroadcastProcessFunction[String, SensorReading, ThresholdUpdate, (String, Double, Double)]#ReadOnlyContext,
                               out: Collector[(String, Double, Double)]): Unit = {

    // get read-only broadcast state,获取只读的广播状态
    val thresholds = readOnlyCtx.getBroadcastState(thresholdStateDescriptor)
    // check if we have a threshold检查阈值是否已经存在
    if (thresholds.contains(reading.id)) {
      // get threshold for sensor 获取指定传感器的阈值
      val sensorThreshold: Double = thresholds.get(reading.id)

      // fetch the last temperature from state从状态中获取上一次的温度
      val lastTemp = lastTempState.value()
      // check if we need to emit an alert检查是否需要发出警报
      val tempDiff = (reading.temperature - lastTemp).abs
      if (tempDiff > sensorThreshold) {
        // temperature increased by more than the threshold，温度增加超过阈值
        out.collect((reading.id, reading.temperature, tempDiff))
      }
    }
    // update lastTemp state，更新lastTemp状态
    this.lastTempState.update(reading.temperature)
  }
}
