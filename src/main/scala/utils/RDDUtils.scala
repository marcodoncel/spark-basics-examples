package utils

import java.net.URI

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.commons.conversions.scala.RegisterJodaTimeConversionHelpers
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import com.mongodb.{DBObject, WriteConcern}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat

import scala.reflect.ClassTag

/**
  * Created by marcodoncel on 3/30/16.
  */
object RDDUtils {

  val S3_SOURCE = 0
  val HBASE_SOURCE = 1
  val MONGO_SOURCE = 2


  def createSparkContext(): SparkContext = {
    val sparkConf = new SparkConf().setAppName(Utils.settings.sparkAppName).set("spark.ui.killEnabled","true")
      .set("spark.default.parallelism", "8")

    if(Utils.settings.sparkMaster != "none")
      sparkConf.setMaster(Utils.settings.sparkMaster)

    new SparkContext(sparkConf)
  }



  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////// VISITORS RDD ////////////////////////////////////////////////////
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def knownVisitorsRDDFromHBase(sc: SparkContext, venueId: Int): RDD[Visitor] ={
    val rdd = getHbaseRDD(sc, HBaseUtils.getHbaseConfiguration(null, venueId, Utils.settings.HBaseKnownVisitorsTableName))
    rdd.map(a => a._2).flatMap(result => result.rawCells()).flatMap(ParserUtils.HBaseRawVisitorToVisitor).cache()
  }


  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////// POINTS RDD ////////////////////////////////////////////////////
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def checkIfS3PathsValid(sc: SparkContext, venueId: Int, queryDate: String) : Boolean ={
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "AKIAIWLXX44JJ575FQJA")
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "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")

    FileSystem.get(new URI("s3n://wifi-points-backup"), sc.hadoopConfiguration).exists(new Path("s3n://wifi-points-backup/"+venueId+"."+queryDate+".gz"))
  }

  def PointsRDDFromSource(sc: SparkContext, queryDate: String, venueId: Int): RDD[Point] = {
    if(checkIfS3PathsValid(sc,venueId,queryDate))
      PointsRDDFromS3(sc,queryDate,venueId)
    else
      PointsRDDFromHBase(sc,queryDate,venueId)
  }

  def PointsRDDFromSource(sc: SparkContext, queryDate: String, venueId: Int, source: Int): RDD[Point] = {

    source match {
      case S3_SOURCE => PointsRDDFromS3(sc,queryDate,venueId)
      case HBASE_SOURCE => PointsRDDFromHBase(sc,queryDate,venueId)
      //case MONGO_SOURCE => PointsRDDFromMongo(sc,queryDate,venueId)
    }
  }

  def PointsRDDFromS3(sc: SparkContext, queryDate: String, venueId: Int): RDD[Point] ={
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "AKIAIWLXX44JJ575FQJA")
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "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")

    //'''Note:''' Because Hadoop's RecordReader class re-uses the same Writable object for each
    //record, directly caching the returned RDD will create many references to the same object.
    //If you plan to directly cache Hadoop writable objects, you should first copy them using a `map` function.
    sc.textFile("s3n://wifi-points-backup/"+venueId+"."+queryDate+".gz").map(_.split(",")).flatMap(mac => mac.tail.flatMap(rawPoint => ParserUtils.CSVPointToPoint(rawPoint,venueId,mac.head))).cache()
  }

  def PointsRDDFromHBase(sc: SparkContext, queryDate: String, venueId: Int): RDD[Point] ={

    val rdd = getHbaseRDD(sc, HBaseUtils.getHbaseConfiguration(queryDate, venueId, Utils.settings.HBaseWifiVisitorsTableName))
    //'''Note:''' Because Hadoop's RecordReader class re-uses the same Writable object for each
    //record, directly caching the returned RDD will create many references to the same object.
    //If you plan to directly cache Hadoop writable objects, you should first copy them using a `map` function.
    rdd.map(a => a._2).flatMap(result => result.rawCells()).flatMap(rawCell => ParserUtils.HBaseRawPointToPoint(rawCell)).cache()
  }

  /*def PointsRDDFromMongo(sc: SparkContext, queryDate: String, venueId: Int): RDD[Point] ={
    //TODO
  }*/

  def getHbaseRDD(sc: SparkContext, configuration: Configuration) = {
    sc.newAPIHadoopRDD(configuration, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
  }




  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////// PROCESS ANALYTICS ////////////////////////////////////////////////////
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  val PROCESS_POINT_ROUTES = false
  val PROCESS_ZONE_ROUTES = true

  def AnalyticsFromRDD(sc: SparkContext, queryDate: String, venueId: Int, pointsRDD: RDD[Point], venueInfo: Broadcast[Map[String,String]], venueLayout: Broadcast[Map[String,String]]): DBObject ={
    //Different devices detected
    val devicesRDD = pointsRDD.map(point => point.beacon).distinct()

    //pointsByMAC
    val pointsXmacRDD = pointsRDD.groupBy(_.beacon)

    //routesByMacRDD IS THE BASE RDD TO MANY ANALYTICS (IT'S CACHED)
    val routesByMacRDD = pointsToRoutesRDD(pointsXmacRDD,PROCESS_POINT_ROUTES,venueLayout)

    //total visitors per point (Number of visitors who has passed by each point)
    val visitorsXpointRDD = visitorsRoutesToPointCounts(routesByMacRDD)
    //total visitors per hour (Number of visitors per hour of the day)
    val visitorsXhourRDD = visitorsRoutesToHourCounts(routesByMacRDD)
    //Total time spent by visitor per zone
    val pointsTotalTimesRDD = pointsToTotalTimeRDD(wifiVisitorsRDDToPointsRDD(routesByMacRDD))
    //AVG Time total (Avg time per each visitor of all his visits)
    val avgTimeXmacRDD = visitorsRoutesToAvgTimeMac(routesByMacRDD)
    //Costumer engagement (Distribution of Number of visitors by time spent in venue)
    val costumerRDD= routesByMacRDD.flatMap(_._2).map(route => (Utils.timeOfRoute(route)/60,1)).reduceByKey(_ + _)
    //UPDATING OLD VISITORS
    val newVSOldVisitorsRDD = updateOldVisitors(sc,routesByMacRDD,queryDate,venueId)


    ///////////ZONES///////////

    //Routes by zone IS THE BASE RDD TO MANY ANALYTICS (IT'S CACHED)
    val routesByZoneByMacRDD = pointsToRoutesRDD(pointsXmacRDD,PROCESS_ZONE_ROUTES,venueLayout)

    //Unique visitors X zone (Number of visitors who has passed by each zone)
    val visitorsXZoneRDD = visitorsRoutesToPointCounts(routesByZoneByMacRDD)
    //Total time spent by visitor per zone
    val zonesTotalTimesRDD = pointsToTotalTimeRDD(wifiVisitorsRDDToPointsRDD(routesByZoneByMacRDD))


    val timetotal = avgTimeXmacRDD.map(_._2).reduce(_ + _)
    val pointsUniqueVisitorsMap = visitorsXpointRDD.collectAsMap()
    val zonesUniqueVisitorsMap = visitorsXZoneRDD.collectAsMap()
    val hoursUniqueVisitorsMap = visitorsXhourRDD.collectAsMap()
    val macsCount = devicesRDD.count()
    val costumerEngagementMap = costumerRDD.collectAsMap()
    val numRealVisitors = routesByMacRDD.count()
    val zonesAvgTimesMap = zonesTotalTimesRDD.collectAsMap()
    val pointsAvgTimesMap = pointsTotalTimesRDD.collectAsMap()


    //val macsjoin = newVisitors.take(1)
    val newmacsjoinCount = newVSOldVisitorsRDD._1.count()
    val oldmacsjoinCount = newVSOldVisitorsRDD._2.count()

    RegisterJodaTimeConversionHelpers()
    var date: DateTime = null
    val formatter = DateTimeFormat.forPattern("yyyyMMdd")
    try {
      date = formatter.parseDateTime(queryDate).withTimeAtStartOfDay()
      MongoDBObject("venue_id" -> 3333,//venueId,
        "date" -> date,
        "processing_time" -> new DateTime().withZone(DateTimeZone.UTC),
        "beacons_detected" -> macsCount,
        "visitors" -> numRealVisitors,
        "new_visitors" -> newmacsjoinCount,
        "old_visitors" -> oldmacsjoinCount,
        "time_total" -> timetotal,
        "visitors_per_section_total" -> zonesUniqueVisitorsMap,
        "visitors_per_point_total" -> pointsUniqueVisitorsMap,
        "visitors_by_time_total" -> costumerEngagementMap,
        "visitors_by_hour_total" -> hoursUniqueVisitorsMap,
        "avg_time_zones" -> zonesAvgTimesMap,
        "avg_time_points" -> pointsAvgTimesMap)
    }
    catch {
      case e:Exception => {
        println(e.getMessage)
        null
      }
    }
  }




  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  /////////////////////////////////////// REDUCING RDD FUNCTIONS //////////////////////////////////////////////////
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////



  def pointsToRoutesRDD(pointsXmacRDD: RDD[(String, Iterable[Point])], convertToZones: Boolean, venueLayout: Broadcast[Map[String,String]]): RDD[(String, List[List[Point]])] = {
    pointsXmacRDD.mapValues(c => RouteOperationsUtils.extractRoutesFromListWifi(c.toList, convertToZones, venueLayout.value))
      //We filter macs that after the process of filtering beacame empty
      .filter(RouteOperationsUtils.filterNoRouteVisitors).cache()
  }

  def visitorsRoutesToPointCounts(visitors: RDD[(String,List[List[Point]])]): RDD[(String,Int)] ={
    visitorRoutesPointMac(visitors)
      .distinct()
      .map(tuple => (tuple._1,1))
      .reduceByKey(_ + _)
  }

  def visitorRoutesPointMac(visitors: RDD[(String,List[List[Point]])]): RDD[(String,String)] = {
    visitors.map(visitor => RouteOperationsUtils.routesToPointMac(visitor._2))
      .flatMap(identity)
  }

  def visitorsRoutesToHourCounts(visitors: RDD[(String,List[List[Point]])]): RDD[(Int,Int)] = {
    visitors.map(visitor => RouteOperationsUtils.pointToMacHour(visitor._2.last.head))
      .distinct()
      .map(tuple => (tuple._2,1))
      .reduceByKey(_ + _)
  }

  def visitorsRoutesToAvgTimeMac(visitors: RDD[(String,List[List[Point]])]): RDD[(String,Long)] = {
    visitors.mapValues(RouteOperationsUtils.routesAverageTime)
      .filter(RouteOperationsUtils.filterWifiVisitor)
  }

  def visitorsToTimeDistribution(visitors: RDD[(String,List[List[Point]])]): RDD[(Long,Int)] = {
    visitors.flatMap(_._2)
      .map(route => (Utils.timeOfRoute(route)/60,1))
      .reduceByKey(_ + _)
  }

  def pointsToTotalTimeRDD(points: RDD[Point]): RDD[(String, Long)] ={
    points.map(point => (point.point,Utils.timeInPoint(point)))
      .reduceByKey(_+_)
  }

  def wifiVisitorsRDDToPointsRDD(visitors: RDD[(String,List[List[Point]])]): RDD[Point] ={
    visitors.map(_._2.flatMap(identity)).flatMap(identity)
  }
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  ///////////////////////////////////////////////// OLD VISITORS //////////////////////////////////////////////////
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////


  def updateOldVisitors(sc: SparkContext, routesByMacRDD: RDD[(String,List[List[Point]])],queryDate: String, venueId: Int): (RDD[Visitor],RDD[Visitor])={
    val hbaseContext = HBaseUtils.getHbaseContext(sc, queryDate, venueId)

    val knownVisitorsRDD = knownVisitorsRDDFromHBase(sc,venueId)

    val newVisitors = routesByMacRDD.leftOuterJoin(knownVisitorsRDD.map(a => (a.mac,a))).filter(_._2._2 == None).map(ParserUtils.tupleToNewVisitor)
    val oldVisitors = routesByMacRDD.leftOuterJoin(knownVisitorsRDD.map(a => (a.mac,a))).filter(_._2._2 != None).map(ParserUtils.updateVisitorFromTuple)

    //inserting new visitors
    /* upsertHbaseOldVisitors(hbaseContext,newVisitors)

     //updating old visitors (for hbase is faster to erase and insert than update)
     upsertHbaseOldVisitors(hbaseContext,oldVisitors)*/

    (newVisitors, oldVisitors)
  }

  def upsertHbaseOldVisitors(hbaseContext: HBaseContext, rdd: RDD[Visitor]) = {
    hbaseContext.bulkPut[Visitor](rdd,
      Utils.settings.HBaseKnownVisitorsTableName,
      (putRecord) => {
        val put = new Put(Bytes.toBytes(putRecord.venue_id.toString))
        put.addColumn(Bytes.toBytes(Utils.settings.HBaseKnownVisitorsCF), Bytes.toBytes(putRecord.mac), Bytes.toBytes(putRecord.firstTimestamp+":"+putRecord.totalVisits+":"+putRecord.lastSeenTimestamp+":"+putRecord.lastVisitTime))
        put
      },
      false)

  }

  def writeRDDToMongo[T:ClassTag](rdd: RDD[T], mongoUri: String)={
    val outputConfig = new Configuration()
    outputConfig.set("mongo.output.uri",
      mongoUri)

    rdd.map(element => (null,element)).saveAsNewAPIHadoopFile("file:///bogus", classOf[Any], classOf[Any], classOf[com.mongodb.hadoop.MongoOutputFormat[Any, Any]], outputConfig)

  }


  // END OLD VISITORS
  def main(args: Array[String]) = {

  }

}
