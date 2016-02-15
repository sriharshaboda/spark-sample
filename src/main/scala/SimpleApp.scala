import java.sql.Timestamp
import java.util.Date
import com.wipro.ats.bdre.md.api.RegisterFile
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

object SimpleApp {
  def main(args: Array[String]) {
    args(0).split(",").foreach(println)
    val logFile = args(0).split(",")(2) // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val lineCount = logData.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _).repartition(1);
    lineCount.saveAsTextFile(args(1))
    val filePath=args(1)+"/part-00000"
    println("filepath ="+ filePath)
    val config = new Configuration()
    val path = new Path(filePath)
    val fileSize = FileSystem.get(config).getFileStatus(path).getLen
    println("filesize= "+ fileSize)
    val fileHash = FileSystem.get(config).getFileChecksum(path).toString
    println("fileHash= "+ fileHash)
    val registerFile = new RegisterFile
    val rfArgs = new Array[String](16)
    rfArgs(0)="-p"
    rfArgs(1)=args(2)
    rfArgs(2)="-sId"
    rfArgs(3)="123461"
    rfArgs(4)="-path"
    rfArgs(5)=filePath
    rfArgs(6)="-fs"
    rfArgs(7)=fileSize.toString
    rfArgs(8)="-fh"
    rfArgs(9)=fileHash.toString
    rfArgs(10)="-cTS"
    rfArgs(11)= new Timestamp(new Date().getTime()).toString
    rfArgs(12)="-bid"
    rfArgs(13)="null"
    rfArgs(14)="--environment-id"
    rfArgs(15)="local1"

    registerFile.execute(rfArgs)

    sc.stop();
  }
}