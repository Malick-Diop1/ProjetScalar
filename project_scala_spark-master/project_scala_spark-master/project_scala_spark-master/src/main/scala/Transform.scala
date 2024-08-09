import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Transform {

  /**
   * Cette fonction permet de nettoyer les données brutes.
   * @param df : Dataframe
   * @return Dataframe
   */
  def cleanData(df: DataFrame): DataFrame = {
    /*
    0. Traiter toutes les colonnes en date timestamp vers YYYY/MM/DD HH:MM SSS.
     */
    val firstDF = df
      .withColumn("event_timestamp", from_unixtime(col("event_timestamp") / 1000000, "yyyy/MM/dd HH:mm:ss.SSS"))
      .withColumn("event_previous_timestamp", from_unixtime(col("event_previous_timestamp") / 1000000, "yyyy/MM/dd HH:mm:ss.SSS"))
      .withColumn("user_first_touch_timestamp", from_unixtime(col("user_first_touch_timestamp") / 1000000, "yyyy/MM/dd HH:mm:ss.SSS"))

    /*
    1. Extraire les revenus d'achat pour chaque événement
      - Ajouter une nouvelle colonne nommée revenue en faisant l'extraction de ecommerce.purchase_revenue_in_usd
     */
    val revenueDF = firstDF.withColumn("revenue", col("ecommerce.purchase_revenue_in_usd"))

    /*
    2. Filtrer les événements dont le revenu n'est pas null
    */
    val purchasesDF = revenueDF.filter(col("revenue").isNotNull)

    /*
    3. Quels sont les types d'événements qui génèrent des revenus ?
      Trouvez des valeurs event_name uniques dans purchasesDF.
      Combien y a-t-il de type d'événements ?
     */
    val distinctDF = purchasesDF.select("event_name").distinct()

    /*
     4. Supprimer la/les colonne(s) inutile(s)
      - Supprimez event_name de purchasesDF.
     */
    val cleanDF = purchasesDF.drop("event_name")

    cleanDF
  }

  /**
   * Cette fonction permet de récupérer le revenu cumulé par source de trafic, par état et par ville
   * @param df : Dataframe
   * @return Dataframe
   */
  def computeTrafficRevenue(df: DataFrame): DataFrame = {
    /*
    5. Revenus cumulés par source de trafic, par état et par ville
      - Obtenir la somme de revenue comme total_rev
      - Obtenir la moyenne de revenue comme avg_rev
     */
    val trafficDF = df.groupBy("traffic_source", "geo.state", "geo.city")
      .agg(
        sum("revenue").alias("total_rev"),
        avg("revenue").alias("avg_rev")
      )

    /*
    6. Récupérer les cinq principales sources de trafic par revenu total
     */
    val topTrafficDF = trafficDF.orderBy(col("total_rev").desc).limit(5)

    /*
    7. Limiter les colonnes de revenus à deux décimales
      Modifier les colonnes avg_rev et total_rev pour les convertir en des nombres avec deux décimales
     */
    val finalDF = topTrafficDF
      .withColumn("total_rev", round(col("total_rev"), 2))
      .withColumn("avg_rev", round(col("avg_rev"), 2))

    finalDF
  }
}
