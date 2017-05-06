import java.io.{BufferedReader, FileReader}

import akka.Done
import akka.actor.{Actor, ActorLogging, ActorSystem, Cancellable, Props}
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.scaladsl.{Keep, Source}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}

import scala.concurrent.Promise

object FileNumberTopic {
  val Topic = "FileNumberTopic"
}


class FileWriter extends Actor with ActorLogging {
  import FileWritermain._
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
      log.info("Initializing...Reading from source file...")
      val producerSettings = ProducerSettings(context.system, new ByteArraySerializer, new StringSerializer)
        .withBootstrapServers("localhost:9092")
      val kafkaSink = Producer.plainSink(producerSettings)
      implicit  val mat =ActorMaterializer()
      Source.unfoldResourceAsync[String, BufferedReader](
        () => Promise.successful(new BufferedReader(new FileReader("D:\\Users\\Atul.Konaje\\IdeaProjects\\FileLoaderMicroservice\\src\\main\\scala\\FileReader\\100Genome_sample_info.csv"))).future,
        reader => Promise.successful(Option(reader.readLine())).future,
        reader => {
          reader.close()
          self ! Stop
          Promise.successful(Done).future
        }).map(new ProducerRecord[Array[Byte], String](FileNumberTopic.Topic, _)).toMat(kafkaSink)(Keep.both).run()

      //val (control, future) = fileSource
      //        future.onFailure {
      //          case exception1 =>
      //            log.error("Stream failed due to error, restarting", exception1)
      //            throw exception1
      //        }
      //      context.become(running(control))
      log.info(s"Writer now running, writing to topic ${FileNumberTopic.Topic}")
      Console println("Writing*************")
  }

     def running(control: Cancellable): Receive = {
      case Stop => {
        log.info("Stopping Kafka producer stream and actor")
        control.cancel()
        context.stop(self)
      }
    }


}
object FileWritermain extends App {
  case object Run
  case object Stop
  val system =ActorSystem("FileLoadingMS")
  val fl=system.actorOf(Props[FileWriter], name="FileWriter")
  println("Done!!")
}