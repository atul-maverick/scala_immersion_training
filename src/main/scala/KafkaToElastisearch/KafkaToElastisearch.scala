package KafkaToElastisearch

import FileReader.FileConsumerMain.system
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods.POST
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpRequest, HttpResponse}
import akka.kafka.ConsumerMessage.{CommittableMessage, CommittableOffsetBatch}
import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.kafka.{ConsumerSettings, Subscriptions}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import akka.kafka.scaladsl.Consumer
import akka.kafka.ConsumerMessage.{CommittableMessage, CommittableOffsetBatch}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import net.liftweb.json._
import net.liftweb.json.Serialization.write
import akka.stream.Materializer

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import akka.kafka.scaladsl.Consumer.Control
import akka.stream.ActorMaterializer
import akka.util.ByteString
import com.typesafe.config.ConfigFactory

/**
  * Created by Atul.Konaje on 5/3/2017.
  */



case class JsonFormatter(Sample:String,Family_ID:String,Population:String,Population_Description:String)//,Gender:String,Relationship:String,Unexpected_Parent_Child:String,Non_Paternity:String,Siblings:String,Grandparents:String,Avuncular:String,Half_Siblings:String,Unknown_Second_Order:String,Third_Order:String,In_Low_Coverage_Pilot:String,LC_Pilot_Platforms:String,LC_Pilot_Centers:String,In_High_Coverage_Pilot:String,HC_Pilot_Platforms:String,HC_Pilot_Centers:String,In_Exon_Targetted_Pilot:String,ET_Pilot_Platforms:String,ET_Pilot_Centers:String,Has_Sequence_in_Phase1:String,Phase1_LC_Platform:String,Phase1_LC_Centers:String,Phase1_E_Platform:String,Phase1_E_Centers:String,In_Phase1_Integrated_Variant_Set:String,Has_Phase1_chrY_SNPS:String,Has_phase1_chrY_Deletions:String,Has_phase1_chrMT_SNPs:String,Main_project_LC_Centers:String,Main_project_LC_platform:String,Total_LC_Sequence:String,LC_Non_Duplicated_Aligned_Coverage:String,Main_Project_E_Centers:String,Main_Project_E_Platform:String,Total_Exome_Sequence:String,X_Targets_Covered_to_20x_or_greater:String,VerifyBam_E_Omni_Free:String,VerifyBam_E_Affy_Free:String,VerifyBam_E_Omni_Chip:String,VerifyBam_E_Affy_Chip:String,VerifyBam_LC_Omni_Free:String,VerifyBam_LC_Affy_Free:String,VerifyBam_LC_Omni_Chip:String,VerifyBam_LC_Affy_Chip:String,LC_Indel_Ratio:String,E_Indel_Ratio:String,LC_Passed_QC:String,E_Passed_QC:String,In_Final_Phase_Variant_Calling:String,Has_Omni_Genotypes:String,Has_Axiom_Genotypes:String,Has_Affy_6_0_Genotypes:String,Has_Exome_LOF_Genotypes:String,EBV_Coverage:String,DNA_Source_from_Coriell:String,Has_Sequence_from_Blood_in_Index:String,Super_Population:String,Super_Population_Description:String)







class KafkaToElastisearch(implicit mat: Materializer) extends  Actor with ActorLogging  {
  import KafkaToEls._
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
      log.info("Reading from the Kafka top Assignment2topic")
      val kafkaendpoint =config.getString("Kafka.endpoint")
      val kafkaport =config.getString("Kafka.port")
      val consumerSettings = ConsumerSettings(context.system, new ByteArrayDeserializer, new StringDeserializer)
        .withBootstrapServers(kafkaendpoint+":"+kafkaport)
        .withGroupId("ForElastisearch")
        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

      val subscription = Subscriptions.topics(KafkaQueueToQueue.Assignment2Topic.Topic)
      val(control,future)= Consumer.committableSource(consumerSettings,subscription)
        .mapAsync(4)(sendToElasatisearch)
        .map(_.committableOffset)
        .toMat(Sink.ignore)(Keep.both) // sink ignore
        .run()
  }

  // outgoing Http connection setup
  implicit val system = ActorSystem("kfktoel")
  val elastipoint =config.getString("elastisearch.endpoint")
  val elastiport =config.getInt("elastisearch.port")
  val elastiCnnFlow: Flow[HttpRequest, HttpResponse, Any] = Http().outgoingConnection(elastipoint,elastiport)
  def elastiRequest(request: HttpRequest): Future[HttpResponse] = akka.stream.scaladsl.Source.single(request).via(elastiCnnFlow).runWith(Sink.head)

  def sendToElasatisearch(msg: Message): Future[Message]={
    // via flow to write to elastic search


    val jsonStr = tojson(msg.record.value())
    val index=msg.record.value().split(",",61)(0).toString().filterNot(_ == '"')
    val handle=config.getString("elastisearch.uri")
    val request = HttpRequest(POST, uri = handle+index, entity = HttpEntity(ContentTypes.`application/json`,ByteString(jsonStr)))

    elastiRequest(request).onComplete(
      status =>
        println(status)
    )
    Future.successful(msg)
  }

  def tojson(strMsg: String): String = {
    val msgArr = strMsg.split(",",61)

    val classObj = JsonFormatter(msgArr(0),msgArr(1),msgArr(2),msgArr(3))

    implicit val formats = DefaultFormats

    val jsonString = write(classObj)


    jsonString
  }

}

object KafkaToEls extends App {
  case object Run
  case object Stop
  val system =ActorSystem("kfktoel")
  val config=ConfigFactory.load()
  implicit  val materializer =ActorMaterializer.create(system)
  type Message = CommittableMessage[Array[Byte], String]
  val fc=system.actorOf(Props(new KafkaToElastisearch()))
  println("Done!!")
} 