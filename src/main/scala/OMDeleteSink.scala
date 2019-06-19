import akka.stream.{Attributes, Inlet, SinkShape}
import akka.stream.stage.{AbstractInHandler, GraphStage, GraphStageLogic}
import com.om.mxs.client.japi.Vault
import org.slf4j.LoggerFactory

/**
  * deletes the given objects (by OID)
  */
class OMDeleteSink(vault:Vault) extends GraphStage[SinkShape[String]]{
  private final val in:Inlet[String] = Inlet.create("OMDeleteSink.in")

  override def shape: SinkShape[String] = SinkShape.of(in)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger = LoggerFactory.getLogger(getClass)

    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        val oid = grab(in)

        val mxsFile = vault.getObject(oid)

        def attemptWithRetry(attempt:Int=0):Unit = {
          try {
            mxsFile.delete()
            pull(in)
          } catch {
            case err:Throwable=>
              logger.error(s"Could not delete file on attempt $attempt", err)
              if(attempt<2){
                Thread.sleep(500)
                attemptWithRetry(attempt+1)
              } else {
                pull(in)
                //failStage(err)
              }
          }
        }

        attemptWithRetry()
      }
    })

    override def preStart(): Unit = pull(in)
  }
}
