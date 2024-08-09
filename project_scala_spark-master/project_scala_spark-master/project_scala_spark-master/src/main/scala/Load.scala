import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode

object Load {

  /**
   * Cette fonction permet de sauvegarder les données dans un répertoire local
   * @param df : Dataframe
   * @param saveMode : méthode de sauvegarde ("overwrite", "append", "ignore", "error")
   * @param format : format des fichiers de sauvegarde ("csv", "parquet", "json", etc.)
   * @param path : chemin où les données doivent être sauvegardées
   * @return : Nothing
   */
  def saveData(df: DataFrame, saveMode: String, format: String, path: String): Unit = {
    val mode = saveMode.toLowerCase match {
      case "overwrite" => SaveMode.Overwrite
      case "append" => SaveMode.Append
      case "ignore" => SaveMode.Ignore
      case "error" => SaveMode.ErrorIfExists
      case _ => throw new IllegalArgumentException(s"Invalid save mode: $saveMode")
    }

    df.write
      .mode(mode)
      .format(format)
      .save(path)
  }

}
