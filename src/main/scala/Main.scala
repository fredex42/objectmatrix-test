import akka.actor.ActorSystem
import akka.stream.scaladsl.{Broadcast, GraphDSL, Merge, RunnableGraph, Sink}
import akka.stream.{ActorMaterializer, Attributes, ClosedShape, Materializer, SourceShape}
import com.om.mxs.client.japi.{Attribute, SearchTerm, UserInfo}
import helpers.ZonedDateTimeEncoder
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global
import io.circe.generic.auto._
import io.circe.syntax._

object Main extends ZonedDateTimeEncoder {
  val logger = LoggerFactory.getLogger(getClass)

  private implicit val actorSystem = ActorSystem("objectmatrix-test")
  private implicit val mat:Materializer = ActorMaterializer.create(actorSystem)

  def terminate(exitCode:Int) = actorSystem.terminate().andThen({
    case _=>System.exit(exitCode)
  })

  def getStream(userInfo:UserInfo, parallelism:Int) = {
    val searchTerm = SearchTerm.createNOTTerm(SearchTerm.createSimpleTerm("oid",""))
    val sinkFactory = Sink.foreach[ObjectMatrixEntry](elem=>
      println(elem.attributes.asJson)
    )

    GraphDSL.create(sinkFactory) { implicit builder=> sink=>
      import akka.stream.scaladsl.GraphDSL.Implicits._
      val src = builder.add(new OMSearchSource(userInfo,searchTerm))
      val splitter = builder.add(new Broadcast[ObjectMatrixEntry](parallelism,true))
      //val lookup = builder.add(new OMLookupMetadata)
      val merger = builder.add(new Merge[ObjectMatrixEntry](parallelism, false))

      src ~> splitter

      for(i<- 0 to parallelism-1) {
        val lookup = builder.add(new OMLookupMetadata().async)
        splitter.out(i) ~> lookup ~> merger.in(i)
      }
      merger ~> sink

      ClosedShape
    }
  }

  def main(args:Array[String]):Unit = {
    UserInfoBuilder.fromFile(args(0)) match {
      case Failure(err)=>
        logger.error(s"Could not connect: ", err)
        terminate(1)
      case Success(userInfo)=>
        logger.info(s"Connected with $userInfo")

        RunnableGraph.fromGraph(getStream(userInfo,4)).run().andThen({
          case Success(noResult)=>
            logger.info(s"Completed stream")
            terminate(0)
          case Failure(err)=>
            logger.error(s"Could not run stream", err)
            terminate(1)

        })
    }
  }
}
