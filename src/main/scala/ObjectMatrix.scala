import com.om.mxs.client.japi.{MatrixStore, UserInfo}

import scala.util.Try

object ObjectMatrix {
  def getConnection(userInfo:UserInfo) = Try {
    MatrixStore.openVault(userInfo)
  }
}
