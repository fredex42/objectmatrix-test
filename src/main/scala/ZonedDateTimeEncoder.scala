package helpers

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import com.om.mxs.client.japi.Vault
import io.circe.Decoder.Result
import io.circe.{Decoder, Encoder, HCursor, Json}

trait ZonedDateTimeEncoder {
  implicit val encodeZonedDateTime: Encoder[ZonedDateTime] = new Encoder[ZonedDateTime] {
    override def apply(a: ZonedDateTime): Json = Json.fromString(a.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME))
  }

  implicit val decodeZonedDateTime: Decoder[ZonedDateTime] = new Decoder[ZonedDateTime] {
    override def apply(c: HCursor): Result[ZonedDateTime] = for {
      str <- c.value.as[String]
    } yield ZonedDateTime.parse(str, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
  }

  implicit val encodeAnyRef: Encoder[AnyRef] = new Encoder[AnyRef] {
    override def apply(a: AnyRef) : Json = Json.fromString("test")
  }

  implicit val encodeVault: Encoder[Vault] = new Encoder[Vault] {
    override def apply(a: Vault): Json = Json.fromString(a.getId)
  }
}
