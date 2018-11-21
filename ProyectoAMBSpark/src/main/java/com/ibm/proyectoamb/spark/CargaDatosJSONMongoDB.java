package com.ibm.proyectoamb.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.mongodb.spark.MongoSpark;
public class CargaDatosJSONMongoDB {
	
	static final String ruta_fichero = "/home/abel/Downloads/ProyectoAMBSpark/src/main/resources";

	public static void main(String[] args) throws InterruptedException {
		
		final SparkConf sparkConf = new SparkConf()
				.setAppName("CargaDatosJSONMongoDB")
				.setMaster("local")
				.set("spark.scheduler.mode", "FAIR")
				.set("spark.scheduler.allocation.file", "/home/abel/Downloads//ProyectoAMBSpark/src/main/resources/conf/conf-scheduler.xml")
				.set("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/ambDB.parcs") // Conexion al Mongo
				.set("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/ambDB.parcs"); //Conexion al Mongo ;
		
		            
		final JavaSparkContext jsc = new JavaSparkContext(sparkConf);   //Carga configuracion
		final SparkSession sqlContext = SparkSession.builder().getOrCreate(); // Carga el Contexto
		
		Dataset<Row> datosMeteorologicos = obtenerDatosXLS(sqlContext);//obtenerDatosXLS(sqlContext); // Spark se conecta al fitxero 
		
		MongoSpark.save(datosMeteorologicos); //Ingesta los datos a Mongo
		
		//OBVIAR AIXO
		
		Dataset<Row> df = MongoSpark.load(jsc).toDF();
		df.printSchema();
		df.show();
		//Thread.sleep(10000000);
// S'ha de posar sempre
		jsc.close();
	}

	private static Dataset<Row> obtenerDatosJSON(final SparkSession sqlContext) {
		final String path = ruta_fichero.concat("/ingrid/carreteras.json");
		final Dataset<Row> datosMeteorologicos = sqlContext.read().json(path);
		return datosMeteorologicos;
	}
	
	private static Dataset<Row> obtenerDatosXLS(final SparkSession sqlContext) {
		final String path = ruta_fichero.concat("/viacar.csv");
		final Dataset<Row> datosMeteorologicos = sqlContext.read()
												.option("useHeader", "true")
										        .format("csv")
												.load(path);
		return datosMeteorologicos;
	}
}
