package KafkaQueueToQueue


import FileReader.FileConsumerMain.system
import FileReader.KafkaConsumerFile
import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.kafka.{ConsumerSettings, ProducerMessage, ProducerSettings, Subscriptions}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, StringDeserializer, StringSerializer}
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.ConsumerMessage.{CommittableMessage, CommittableOffsetBatch}
import akka.stream.scaladsl.{Keep, Sink}

import scala.concurrent.Future
import akka.kafka.scaladsl.Consumer.Control
import akka.stream.ActorMaterializer
import org.apache.kafka.clients.producer.ProducerRecord
/**
  * Created by Atul.Konaje on 5/2/2017.
  */
class QueueToQueue extends  Actor with ActorLogging  {
  import QTOQ._

  override def preStart(): Unit ={
    super.preStart()
    self ! Run
  }

  override def postStop(): Unit = {
    super.postStop()
    println("Producer stopped")
  }

  override def receive: Receive = {
    case Run =>

      val producerSettings = ProducerSettings(context.system, new ByteArraySerializer, new StringSerializer)
        .withBootstrapServers("localhost:9092")
      val kafkaSink = Producer.plainSink(producerSettings)
      implicit  val mat =ActorMaterializer()
      val consumerSettings = ConsumerSettings(context.system, new ByteArrayDeserializer, new StringDeserializer)
        .withBootstrapServers("localhost:9092")
        .withGroupId("Filenumberc")
        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      Consumer.committableSource(consumerSettings, Subscriptions.topics(FileReader.FileNumberTopic.Topic))
        .map { msg =>
          println(s"topic1 -> topic2: $msg")
          val processedmsg=msg.record.value.toUpperCase()
          ProducerMessage.Message(new ProducerRecord[Array[Byte], String](
            "Assignment2topic",
            processedmsg
          ), msg.committableOffset)
        }
        .runWith(Producer.commitableSink(producerSettings))
  }


}


object QTOQ extends App {
  case object Run
  case object Stop
  val system =ActorSystem("qtoq")
  type Message = CommittableMessage[Array[Byte], String]
  val fc=system.actorOf(Props[QueueToQueue], name="quetoque")
  println("Done!!")
}
