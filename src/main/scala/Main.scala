import java.io.File
import java.time.{Instant, ZoneId, ZonedDateTime}
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Broadcast, FileIO, GraphDSL, Merge, RunnableGraph, Sink}
import akka.stream.{ActorMaterializer, Attributes, ClosedShape, Materializer, SourceShape}
import akka.util.ByteString
import com.om.mxs.client.japi.{Attribute, Constants, MatrixStore, SearchTerm, UserInfo}
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

  def attributesToCSV(maybeAttribs: Option[MxsMetadata]) = maybeAttribs match {
    case None=>"\n"
    case Some(attribs)=>
      val allValues = List("MXFS_FILENAME_UPPER","GNM_PROJECT_ID","MXFS_USERNAME","MXFS_PATH","MXFS_FILENAME","MXFS_DESCRIPTION")
        .map(key=>attribs.stringValues.getOrElse(key, "-"))
        .map(str=>"\"" + str + "\"") ++
      List("MXFS_INTRASH").map(key=>attribs.boolValues.get(key)
        .map(value=>if(value) "true" else "false").getOrElse("-")) ++
      List("MXFS_MODIFICATION_TIME","MXFS_CREATION_TIME","MXFS_ACCESS_TIME","MXFS_ARCHIVE_TIME")
        .map(key=>attribs.longValues.get(key)
          .map(value=>ZonedDateTime.ofInstant(Instant.ofEpochMilli(value), ZoneId.systemDefault()).toString).
          getOrElse("-")) ++
      List("DPSP_SIZE").map(key=>attribs.longValues.get(key).map(_.toString).getOrElse("-")) ++
      List("MXFS_CREATIONDAY","MXFS_ARCHMONTH","MXFS_COMPATIBLE","MXFS_ARCHYEAR","MXFS_ARCHDAY","MXFS_CREATIONMONTH","MXFS_CREATIONYEAR")
        .map(key=>attribs.intValues.get(key).map(_.toString).getOrElse("-"))

      allValues.mkString(",")+"\n"
  }

//  val interestingFields = Array(
//    "MXFS_FILENAME_UPPER","GNM_PROJECT_ID","MXFS_USERNAME","MXFS_PATH","MXFS_FILENAME","MXFS_DESCRIPTION",
//    "MXFS_INTRASH","MXFS_MODIFICATION_TIME","MXFS_CREATION_TIME","MXFS_ACCESS_TIME","MXFS_ARCHIVE_TIME",
//    "DPSP_SIZE","MXFS_CREATIONDAY","MXFS_ARCHMONTH","MXFS_COMPATIBLE","MXFS_ARCHYEAR","MXFS_ARCHDAY","MXFS_CREATIONMONTH","MXFS_CREATIONYEAR"
//  )

  val interestingFields = Array(
    "MXFS_FILENAME_UPPER","MXFS_FILENAME",
  )
  def getStream(userInfo:UserInfo, parallelism:Int, maybeSearchParam:Option[String]) = {
    //val searchTerm = SearchTerm.createNOTTerm(SearchTerm.createSimpleTerm("oid",""))

    val searchTerm = Seq(
      maybeSearchParam.getOrElse("*"),
      s"keywords: ${interestingFields.mkString(",")}").mkString("\n")
    val sinkFactory = FileIO.toPath(new File("report.csv").toPath)

    GraphDSL.create(sinkFactory) { implicit builder=> sink=>
      import akka.stream.scaladsl.GraphDSL.Implicits._
      val src = builder.add(new OMFastContentSearchSource(userInfo,searchTerm))

      src.out
        .map(entry=>attributesToCSV(entry.attributes))
        .map(line=>ByteString(line)) ~> sink
      ClosedShape
    }
  }

  def main(args:Array[String]):Unit = {
    try {
    UserInfoBuilder.fromFile(args(0)) match {
      case Failure(err)=>
        logger.error(s"Could not connect: ", err)
        terminate(1)
      case Success(userInfo)=>
        logger.info(s"Connected with $userInfo")

        val searchParam = if(args.length>1) {
          Some(args(1))
        } else {
          None
        }

        RunnableGraph.fromGraph(getStream(userInfo,1, searchParam)).run().andThen({
          case Success(_)=>
            logger.info(s"Completed stream")
            terminate(0)
          case Failure(err)=>
            logger.error(s"Could not run stream", err)
            terminate(1)

        })

    }
  } catch {
      case err:Throwable=>
        logger.error("",err)
        terminate(255)
    }
  }
}
