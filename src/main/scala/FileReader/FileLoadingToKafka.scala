package FileReader
import java.io.{BufferedReader, FileReader}

import akka.Done
import akka.actor.FSM.Failure
import akka.actor.{Actor, ActorLogging, ActorSystem, Cancellable, Props}
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.scaladsl.{Keep, Source}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future, Promise}




class FileWriter extends Actor with ActorLogging {
  import FileWritermain._
  override def preStart(): Unit ={
    super.preStart()
    self ! Run
  }

  override def postStop(): Unit = {
    super.postStop()
    context.system.terminate()
    println("Producer stopped")
  }


  override def receive: Receive = {
    case Run =>
      log.info("Initializing...Reading from source file...")
      val kafkaendpoint =config.getString("Kafka.endpoint")
      val kafkaport =config.getString("Kafka.port")
      val producerSettings = ProducerSettings(context.system, new ByteArraySerializer, new StringSerializer)
        .withBootstrapServers("localhost:9092")
      val kafkaSink = Producer.plainSink(producerSettings)
      implicit  val mat =ActorMaterializer()
      val input_file=config.getString("datainput.file")
      val kafka_topic=config.getString("KafkaTopic.topic1")
     Source.unfoldResourceAsync[String, BufferedReader](
        () => Promise.successful(new BufferedReader(new FileReader(input_file))).future,
        reader => Promise.successful(Option(reader.readLine())).future,
        reader => {
          reader.close()
          Promise.successful(Done).future
        }).map(new ProducerRecord[Array[Byte], String](kafka_topic, _)).toMat(kafkaSink)(Keep.both).run()
      log.info(s"Currently writing to topic ${kafka_topic}")
      Console println("Writing*************")



  }



}
object FileWritermain extends App {
  case object Run
  case object Stop
  val system =ActorSystem("FileLoadingMS")
  val config = ConfigFactory.load()
  val fl=system.actorOf(Props[FileWriter], name="FileWriter")
  println("Done!!")
}