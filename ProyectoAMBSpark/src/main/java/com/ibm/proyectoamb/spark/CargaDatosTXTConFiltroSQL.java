package com.ibm.proyectoamb.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.ibm.proyectoamb.spark.bean.DatosMeteorologicos;

public class CargaDatosTXTConFiltroSQL {
	
	static final String ruta_fichero = "/home/abel/Downloads/ProyectoAMBSpark/src/main/resources";

	public static void main(String[] args) {
		
		final SparkConf sparkConf = new SparkConf().setAppName("CargaDatosTXTConFiltroSQL").setMaster("local");
		final JavaSparkContext spark = new JavaSparkContext(sparkConf);
		final SparkSession sqlContext = SparkSession.builder().getOrCreate();

		Dataset<Row> datosMeteorologicos = obtenerDatos(sqlContext);
		final Dataset<DatosMeteorologicos> as = datosMeteorologicos.as(Encoders.bean(DatosMeteorologicos.class));

		as.createOrReplaceTempView("datos");
		datosMeteorologicos = sqlContext.sql("select date_format(to_date(Time,'yyyyMMdd'),'dd-MM-yyyy') as fecha from datos where Indoor_Temperature < 1");
		datosMeteorologicos.show();
//		salvarDatos(datosMeteorologicos);
//		spark.close();
	}
	
	private static void salvarDatos(final Dataset<Row> datosMeteorologicos) {
		datosMeteorologicos.write().json(ruta_fichero.concat("/json"));
		System.out.println("Arxiu JSON guardat");
	}

	private static Dataset<Row> obtenerDatos(final SparkSession sqlContext) {
		final String path = ruta_fichero.concat("/datosMeteorologicos.txt");
		final Dataset<Row> datosMeteorologicos = sqlContext.read().format("com.databricks.spark.csv").option("inferSchema", "true")
				.option("header", "true").option("delimiter", "\t").load(path);
		return datosMeteorologicos;
	}
}
