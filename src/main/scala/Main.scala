import akka.actor.ActorSystem
import akka.stream.scaladsl.{GraphDSL, RunnableGraph, Sink}
import akka.stream.{ActorMaterializer, ClosedShape, Materializer, SourceShape}
import com.om.mxs.client.japi.{Attribute, SearchTerm, UserInfo}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global

object Main {
  val logger = LoggerFactory.getLogger(getClass)

  private implicit val actorSystem = ActorSystem("objectmatrix-test")
  private implicit val mat:Materializer = ActorMaterializer.create(actorSystem)

  def terminate(exitCode:Int) = actorSystem.terminate().andThen({
    case _=>System.exit(exitCode)
  })

  def getStream(userInfo:UserInfo) = {
    val searchTerm = SearchTerm.createNOTTerm(SearchTerm.createSimpleTerm("oid",""))
    val sinkFactory = Sink.foreach[ObjectMatrixEntry](elem=>logger.info(s"Got $elem"))

    GraphDSL.create(sinkFactory) { implicit builder=> sink=>
      import akka.stream.scaladsl.GraphDSL.Implicits._
      val src = builder.add(new OMSearchSource(userInfo,searchTerm))
      val lookup = builder.add(new OMLookupMetadata)

      src ~> lookup ~> sink

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

        val graph = RunnableGraph.fromGraph(getStream(userInfo)).run().andThen({
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
