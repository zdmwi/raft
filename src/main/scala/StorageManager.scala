import java.io.*


object StorageManager {
  def serialize(obj: Any, filePath: String): Boolean =
    try {
      val fileOutputStream = FileOutputStream(filePath)
      val objectOutputStream = ObjectOutputStream(fileOutputStream)
      objectOutputStream.writeObject(obj)
      objectOutputStream.flush()
      objectOutputStream.close()
      fileOutputStream.close()
      true
    } catch {
      case _: FileNotFoundException =>
        false
      case e: Exception =>
        println(s"An error occurred ${e.getMessage}")
        false
    }

  def deserialize[T](filePath: String): Option[T] =
    try {
      val fileInputStream = FileInputStream(filePath)
      val objectInputStream = ObjectInputStream(fileInputStream)
      val obj = objectInputStream.readObject()
      objectInputStream.close()
      fileInputStream.close()
      Some(obj.asInstanceOf[T])
    } catch {
      case _: FileNotFoundException =>
        None
      case e: Exception =>
        println(s"An error occurred ${e.getMessage}")
        None
    }
}

