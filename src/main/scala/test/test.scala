package test

import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.KryoSerializer
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.SpatialRDD


/**
  * Created by song on 2016/12/1.
  * 测试环境
  * java 1.7.0_51
  * scala 2.10.4
  * geospark 1.1.3
  */
object test {
    def main(args: Array[String]): Unit = {
        /**
          * Init Spark
          */
        val conf = new SparkConf()
        conf.setAppName("GeoSparkRunnableExample") // Change this to a proper name
        conf.setMaster("local[*]") // Delete this if run in cluster mode
        // Enable GeoSpark custom Kryo serializer
        conf.set("spark.serializer", classOf[KryoSerializer].getName)
        conf.set("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
        val sc = new SparkContext(conf)

        /**
          * ShapeFile Location
          * The file extensions of .shp, .shx, .dbf must be in lowercase. Assume you have a shape file called myShapefile, the file structure should be like this:
          * - shapefile1
          * - shapefile2
          * - myshapefile
          *     - myshapefile.shp
          *     - myshapefile.shx
          *     - myshapefile.dbf
          *     - myshapefile...
          * - ...
          */
        val shapefileInputLocation="E:\\workspace\\songruoningbupt-geoSpark\\data\\test\\vector\\xj"

        /**
          * test1
          * java 1.7.0_51
          * 这里报
          * Exception in thread "main" java.lang.UnsupportedClassVersionError: org/opengis/referencing/FactoryException : Unsupported major.minor version 52.0
          * at java.lang.ClassLoader.defineClass1(Native Method)
          * at java.lang.ClassLoader.defineClass(ClassLoader.java:800)
          * at java.security.SecureClassLoader.defineClass(SecureClassLoader.java:142)
          * at java.net.URLClassLoader.defineClass(URLClassLoader.java:449)
          * at java.net.URLClassLoader.access$100(URLClassLoader.java:71)
          * at java.net.URLClassLoader$1.run(URLClassLoader.java:361)
          * at java.net.URLClassLoader$1.run(URLClassLoader.java:355)
          * at java.security.AccessController.doPrivileged(Native Method)
          * at java.net.URLClassLoader.findClass(URLClassLoader.java:354)
          * at java.lang.ClassLoader.loadClass(ClassLoader.java:425)
          * at sun.misc.Launcher$AppClassLoader.loadClass(Launcher.java:308)
          * at java.lang.ClassLoader.loadClass(ClassLoader.java:358)
          * at test.test$.main(test.scala:41)
          *
          * test2
          * File->setting->Java Compiler =>> 1.8
          * java 1.8.0_172
          * 跑通
          *
          * test3
          * 回归测试
          * File->setting->Java Compiler =>> 1.7
          * java 1.7.0_51
          * 依旧不行
          *
          *
          */
        var spatialRDD = new SpatialRDD[Geometry]
        spatialRDD.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sc, shapefileInputLocation)

        println(spatialRDD.getSampleNumber)

        sc.stop()
    }
}
