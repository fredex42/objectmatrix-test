import com.om.mxs.client.japi.{MXFSFileAttributes, Vault}

case class ObjectMatrixEntry(oid:String, vault:Vault, attributes:Option[MxsMetadata], fileAttribues:Option[FileAttributes])