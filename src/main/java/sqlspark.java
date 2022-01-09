import java.io.IOException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.util.Scanner;
import static org.apache.spark.sql.functions.*;

/*
 * Desarrollado por: Jose Manuel R
 * */
public class sqlspark {
	
	public void secondAlgExe(String In, String input) throws IOException {
		// Create Spark Session to create connection to Spark
		final SparkSession sparkSession = SparkSession.builder().appName("Big Data Process Spark and CSV files")
				.master("local[*]").getOrCreate();

		//Eliminaci�n mensajes de log en la consola
		sparkSession.sparkContext().setLogLevel("ERROR");
		String us = In;

		// Creo un nuevo esquema para los datos que voy a introducir
		/*StructType customStructType = new StructType();
		customStructType = customStructType.add("age", DataTypes.IntegerType, false);
		customStructType = customStructType.add("job", DataTypes.StringType, false);
		customStructType = customStructType.add("marital", DataTypes.StringType, false);
		customStructType = customStructType.add("education", DataTypes.StringType, false);
		customStructType = customStructType.add("default", DataTypes.StringType, false);
		customStructType = customStructType.add("balance", DataTypes.IntegerType, false);
		customStructType = customStructType.add("housing", DataTypes.StringType, false);
		customStructType = customStructType.add("loan", DataTypes.StringType, false);
		customStructType = customStructType.add("contact", DataTypes.StringType, false);
		customStructType = customStructType.add("day", DataTypes.IntegerType, false);
		customStructType = customStructType.add("month", DataTypes.StringType, false);
		customStructType = customStructType.add("duration", DataTypes.IntegerType, false);
		customStructType = customStructType.add("campaign", DataTypes.IntegerType, false);
		customStructType = customStructType.add("pdays", DataTypes.StringType, false);
		customStructType = customStructType.add("poutcome", DataTypes.StringType, false);
		customStructType = customStructType.add("deposit", DataTypes.StringType, false);*/
		
		Dataset<Row> dflect = sparkSession.read()
				.format("csv")
				.option("header", "true")
				.option("delimiter", ";")
				//.schema(customStructType)
				.load(us);
		
		// Se crea una vista para obtener columnas y datos
		dflect.createOrReplaceTempView("TRAINING_DATA");
		//dflect.show(false);
		/*final Dataset<Row> typedTrainingData = sparkSession.sql(
				"SELECT CAST(age as int) age, CAST(job as String) job, CAST(marital as String) marital, "
						+ "CAST(education as String) education, CAST(default as String) default, CAST(balance as int) balance, "
						+ "CAST(housing as String) housing, CAST(loan as String) loan, CAST(contact as String) contact, CAST(day as int) day, "
						+ "CAST(month as String) month, CAST(duration as int) duration, CAST(campaign as int) campaign, CAST(pdays as String) pdays,"
						+ "CAST(poutcome as String) poutcome, CAST(deposit as String) deposit FROM TRAINING_DATA"); // CAST(vel_Media
*/

		int number = Integer.parseInt(input);
		switch (number) 
        {
            case 1:
            	System.out.println("Rango de edad qu� contrata m�s prestamos");
            	//Rango de edad qu� contrata m�s prestamos
            	Dataset<Row> sqlResult = sparkSession
                    .sql("select count(age) as CantidadPrestamos, age from TRAINING_DATA WHERE loan = 'yes' GROUP BY age ORDER BY CantidadPrestamos desc");
            	sqlResult.show(false);
                break;
            case 2:
        		//�Cu�l es el rango edad y estado civil que tiene m�s dinero en las cuentas?
            	System.out.println("Rango edad y estado civil que tiene m�s dinero en las cuentas");
            	Dataset<Row> sqlrangoeec = sparkSession
                        .sql("select age, marital, SUM(balance) as AccountBalance"
                        		+ " from TRAINING_DATA GROUP BY age, marital ORDER BY AccountBalance desc limit 5");
        		sqlrangoeec.show(false);
                break;
            case 3:  
            	//�Cu�l es la forma m�s com�n de contactar a los clientes, entre 25-35 a�os?
        		//se contacta a trav�s de la columna contact
            	System.out.println("Forma m�s com�n de contactar a los clientes, entre 25-35 a�os");
            	Dataset<Row> sqlContact = sparkSession
                        .sql("select age, contact, count(contact) as TotalClientesxEdad"
                        		+ " from TRAINING_DATA where age > 24 and age < 36 GROUP BY age, contact ORDER BY Age desc");
        		sqlContact.show(false);
                break;
            case 4:  
            	//�Cu�l es el balance medio, m�ximo y minimo por cada tipo de campa�a, teniendo en
        		//cuenta su estado civil y profesi�n?
            	System.out.println("Balance medio, m�ximo y minimo por cada tipo de campa�a, teniendo en cuenta su estado civil y profesi�n");
        		Dataset<Row> sqlCampaign = sparkSession
                        .sql("select avg(balance) as BalanceMedio, min(balance) as BalanceMinimo, max(balance) as BalanceMaximo, marital, job"
                        		+ " from TRAINING_DATA GROUP BY marital, job ORDER BY BalanceMedio desc");
        		sqlCampaign.show(false);
                break;
            case 5:  
            	//�Cu�l es el tipo de trabajo m�s com�n, entre los casados (job=married), que tienen
        		//casa propia (housing=yes), y que tienen en la cuenta m�s de 1.200� y qu� son de la
        		//campa�a 3?
            	System.out.println("Tipo de trabajo m�s com�n, entre los casados (job=married), que tienen casa propia (housing=yes), y que tienen en la cuenta m�s de 1.200� y qu� son de la campa�a 3");
        		Dataset<Row> sqlJob = sparkSession
                        .sql("select job, count(job) as jobcount, marital"
                        		+ " from TRAINING_DATA WHERE (marital='married') and (housing='yes') and (balance>1200) and (campaign=3)"
                        		+ "GROUP BY marital, job "
                        		+ "ORDER BY jobcount desc");
        		sqlJob.show(false);
                break;
			case 6:
				//Pivot
				//Agrupamos por lo que queramos, en este caso solo por edad
				//Se pivota por la columna marital que contiene los valores:
				// - divorced
				// - married
				// - single
				//Y se aplica la operación sum (puede ser otra como avg u otras) sobre la columna balance
				//Por lo tanto nos da agrupado por age y pivotado sobre la columna marital la suma por edad diviendo por el estado marital (estado civil -> divorciado, casado o soltero)
				Dataset<Row> sqlPivot = sparkSession
						.sql("select *"
								+ " from TRAINING_DATA");
				sqlPivot.groupBy(col("age"))
						.pivot("marital")
						.agg(avg(col("balance"))).show(false);
				break;
           
            default: 
            	System.out.println("Entrada incorrecta");
                break;
        }		
	}
	
	public static void main(String[] args) throws IOException {
	
		String in = "C:\\Users\\jose_\\Desktop\\sqlspark.csv"; // donde se carga el CSV
		sqlspark d = new sqlspark();
		System.out.println("Bienvenido, inserte el nº correspondiente a la consulta que quiera hacer: \n"
				+ "1. Rango de edad que contrata más prestamos \n"
				+ "2. Rango de edad y estado civil que tiene más dinero en las cuentas \n"
				+ "3. Forma más común de contactar a los clientes entre 25-35 años \n"
				+ "4. Balance medio, máximo y minimo por cada tipo de campaña, teniendo en cuenta su estado civil y profesión"
				+ "5. Tipo de trabajo más común, entre los casados (job=married), que tienen casa propia (housing=yes), y que tienen en la cuenta más de 1.200€ y que son de la campaña 3 \n");
		System.out.println("(Para finalizar el programa escriba 'exit'");
		System.out.println("Introduce el número: ");
		Boolean exit = false;
		while (!exit) {
			Scanner keyboard = new Scanner(System.in);
			String input = keyboard.nextLine();
			System.out.println("Ha introducido: " + input);
			if (!input.equals("exit")) {
				d.secondAlgExe(in, input);
			} else {
				exit = true;
				System.out.println("Programa finalizado.");
				System.exit(1);
			}
		System.out.println("Introduzca otro número o escriba 'exit' para finalizar el programa.");
		}
	}
}