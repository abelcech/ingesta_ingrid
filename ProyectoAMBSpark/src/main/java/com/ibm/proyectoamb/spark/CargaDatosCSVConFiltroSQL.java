package com.ibm.proyectoamb.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.ibm.proyectoamb.spark.bean.Carreteres;

public class CargaDatosCSVConFiltroSQL {	
	static final String ruta_fichero = "/home/abel/Downloads/ProyectoAMBSpark/src/main/resources";
    static final String arxivo="/viatun.csv"; // %Name of the file with \ in front

public static void main(String[] args) {
	
	final SparkConf sparkConf = new SparkConf().setAppName("CargaDatosCSVConFiltroSQL").setMaster("local");
	final JavaSparkContext spark = new JavaSparkContext(sparkConf);
	final SparkSession sqlContext = SparkSession.builder().getOrCreate();

	Dataset<Row> carreteres = obtenerDatos(sqlContext,arxivo);
//	carreteres = carreteres.select("ide").alias("nombre");
carreteres.show();
	carreteres=renameColumns(carreteres,arxivo);
	carreteres.show();
	if(arxivo=="/viacar.csv") {
	final Dataset<Carreteres> as = carreteres.as(Encoders.bean(Carreteres.class));
	as.createOrReplaceTempView("datos");
	}
	if(arxivo=="/viatun.csv") {
		final Dataset<Carreteres2> as = carreteres.as(Encoders.bean(Carreteres2.class));
		as.createOrReplaceTempView("datos");
		}
	//
	
	carreteres = sqlContext.sql("select ide, date_format(to_date(cast(dataalta as String),'yyyyMMdd'),'dd-MM-yyyy') as dataalta from datos where ide > 893");
//    carreteres = sqlContext.sql("select ide,codigo from datos where ide > 893");
	carreteres.show(10);    
	//salvarDatos(carreteres);
	spark.close();
}

private static void salvarDatos(final Dataset<Row> carreteres) {
	carreteres.write().json(ruta_fichero.concat("/json"));
	System.out.println("Arxiu JSON guardat");
}

private static Dataset<Row> obtenerDatos(final SparkSession sqlContext, String arxiu) {
	final String path = ruta_fichero.concat(arxiu);
	final Dataset<Row> carreteres = sqlContext.read().format("com.databricks.spark.csv").option("inferSchema", "true")
			.option("header", "true").option("delimiter", ";").load(path);
	return carreteres;
}
private static Dataset<Row> renameColumns(Dataset<Row> carreteres, String arxivo) {
	if(arxivo=="/viacar.csv") {
carreteres=carreteres.withColumnRenamed("Codigo", "codigo");
carreteres=carreteres.withColumnRenamed("Resumen", "resumen");
carreteres=carreteres.withColumnRenamed("Ascendente", "ascendente");
carreteres=carreteres.withColumnRenamed("Familia", "familia");
carreteres=carreteres.withColumnRenamed("Cantidad", "cantidad");
carreteres=carreteres.withColumnRenamed("Unidad", "unidad");
carreteres=carreteres.withColumnRenamed("DataDeteccio", "datadeteccio");
carreteres=carreteres.withColumnRenamed("DataAlta", "dataalta");
carreteres=carreteres.withColumnRenamed("DataBaixa", "databaixa");
carreteres=carreteres.withColumnRenamed("PKinicialkmm", "pkinicialkmm");
carreteres=carreteres.withColumnRenamed("PKfinalkmm", "pkfinalkmm");
carreteres=carreteres.withColumnRenamed("Utmx", "utmx");
carreteres=carreteres.withColumnRenamed("Utmy", "utmy");
carreteres=carreteres.withColumnRenamed("ElementreferenciaText", "elementreferenciatext");
carreteres=carreteres.withColumnRenamed("Calzada15", "calzada_lloc");
carreteres=carreteres.withColumnRenamed("Municipi", "municipi");
carreteres=carreteres.withColumnRenamed("Quilometres", "quilometres");
carreteres=carreteres.withColumnRenamed("Ampletotalmitja", "ampletotalmitja");
carreteres=carreteres.withColumnRenamed("Via", "via");
carreteres=carreteres.withColumnRenamed("Categoria", "categoria");
carreteres=carreteres.withColumnRenamed("Longitudm", "longitudm");
carreteres=carreteres.withColumnRenamed("Calzada22", "calzada_tipus");
carreteres=carreteres.withColumnRenamed("Ndetrams", "ndetrams");
carreteres=carreteres.withColumnRenamed("Ncarrilscalzadaesq.", "ncarrilscalzadaesq");
carreteres=carreteres.withColumnRenamed("Ncarrilscalzadadreta.", "ncarrilscalzadadreta");
carreteres=carreteres.withColumnRenamed("Vialenta", "vialenta");
carreteres=carreteres.withColumnRenamed("Voralspavimentats", "voralspavimentats");
carreteres=carreteres.withColumnRenamed("Long.riscdegeladesm", "long_riscdegeladesm");
carreteres=carreteres.withColumnRenamed("Long.riscdenevadesm", "long_riscdenevadesm");
carreteres=carreteres.withColumnRenamed("Zonaclimatica", "zonaclimatica");
	}
	if(arxivo=="/viatun.csv") {
		carreteres=carreteres.withColumnRenamed("Codigo", "codigo");
		carreteres=carreteres.withColumnRenamed("Resumen", "resumen");
		carreteres=carreteres.withColumnRenamed("Ascendente", "ascendente");
		carreteres=carreteres.withColumnRenamed("Familia", "familia");
		carreteres=carreteres.withColumnRenamed("Cantidad", "cantidad");
		carreteres=carreteres.withColumnRenamed("Unidad", "unidad");
		carreteres=carreteres.withColumnRenamed("DataDeteccio", "datadeteccio");
		carreteres=carreteres.withColumnRenamed("DataAlta", "dataalta");
		carreteres=carreteres.withColumnRenamed("DataBaixa", "databaixa");
		carreteres=carreteres.withColumnRenamed("PKinicialkmm", "pkinicialkmm");
		carreteres=carreteres.withColumnRenamed("PKfinalkmm", "pkfinalkmm");
		carreteres=carreteres.withColumnRenamed("Utmx", "utmx");
		carreteres=carreteres.withColumnRenamed("Utmy", "utmy");
		carreteres=carreteres.withColumnRenamed("ElementreferenciaText", "elementreferenciatext");
		carreteres=carreteres.withColumnRenamed("Calzada", "calzada");
		carreteres=carreteres.withColumnRenamed("LongitudAm", "longitudam");
		carreteres=carreteres.withColumnRenamed("ParetslateralsAm2", "paretslateralsam2");
		carreteres=carreteres.withColumnRenamed("VoltaAm2", "voltaam2");
		carreteres=carreteres.withColumnRenamed("Revestimentfuncionalenparetslaterals_Am2", "revestimentfuncionalenparetslaterals_am2");
		carreteres=carreteres.withColumnRenamed("Laminaimpermeabilitzantenvolta_Am2", "laminaimpermeabilitzantenvolta_am2");
		carreteres=carreteres.withColumnRenamed("GalibA", "galiba");
		carreteres=carreteres.withColumnRenamed("Longitud_Bm", "longitud_bm");
		carreteres=carreteres.withColumnRenamed("ParetslateralsBm2", "paretslateralsbm2");
		carreteres=carreteres.withColumnRenamed("VoltaBm2", "voltabm2");
		carreteres=carreteres.withColumnRenamed("RevestimentfuncionalenparetslateralsBm2", "revestimentfuncionalenparetslateralsbm2");
		carreteres=carreteres.withColumnRenamed("LaminaimpermeabilitzantenvoltaBm2", "laminaimpermeabilitzantenvolta_bm2");
		carreteres=carreteres.withColumnRenamed("GalibB", "galibb");
		carreteres=carreteres.withColumnRenamed("Longitudm", "longitudm");
		carreteres=carreteres.withColumnRenamed("Paretslateralsm2", "paretslateralsm2");
		carreteres=carreteres.withColumnRenamed("Voltam2", "voltam2");
		carreteres=carreteres.withColumnRenamed("Revestimentfuncionalenparetslateralsm2", "revestimentfuncionalenparetslateralsm2");
		carreteres=carreteres.withColumnRenamed("Laminaimpermeabilitzantenvoltam2", "laminaimpermeabilitzantenvoltam2");
		carreteres=carreteres.withColumnRenamed("Galibm", "galibm");
		carreteres=carreteres.withColumnRenamed("Centredecontroldelfuncionamentdelesinstal·lacions", "centredecontroldelfuncionamentdelesinstallacions");
		carreteres=carreteres.withColumnRenamed("Enllumenat", "enllumenat");
		carreteres=carreteres.withColumnRenamed("Ventilacio", "ventilacio");
		carreteres=carreteres.withColumnRenamed("Seguretat", "seguretat");
		carreteres=carreteres.withColumnRenamed("TVencircuittancat", "tvencircuittancat");
		carreteres=carreteres.withColumnRenamed("Nd'extintors", "nd_extintors");
		carreteres=carreteres.withColumnRenamed("Palsd'auxili", "palsd_auxili");
		carreteres=carreteres.withColumnRenamed("Instal·lacionsemaforica", "installacionsemaforica");
		carreteres=carreteres.withColumnRenamed("Senyalsdemissatgevariable", "senyalsdemissatgevariable");
		carreteres=carreteres.withColumnRenamed("Controldegalib", "controldegalib");
		carreteres=carreteres.withColumnRenamed("TipusSeparacio", "tipusseparacio");




			}
return carreteres;	
}
}


