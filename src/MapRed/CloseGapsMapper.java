package MapRed;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import entities.CeldaWritable;

public class CloseGapsMapper extends Mapper<LongWritable, Text, CeldaWritable, DoubleWritable> {

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {

		final String linea = ivalue.toString();
		String[] datos = datosXLinea(linea);
		int col = Integer.valueOf(datos[0].split(",")[0]);
		int fila = Integer.valueOf(datos[0].split(",")[1]);
		double rend = Double.valueOf(datos[1]);

		// La celda se reporta (con su rendimiento) como vecino

		// "Vecino de si mismo"
		// devuelvo la misma celda, con rend*-1 para diferenciarlo: si hay rend neg,
		// entonces esa celda no te3ngo que rellenarla
		CeldaWritable celda = new CeldaWritable(new IntWritable(fila), new IntWritable(col));
		context.write(celda, new DoubleWritable(rend*-1));

		// Vecino de la "celda de arriba"
		celda = new CeldaWritable(new IntWritable(fila + 1), new IntWritable(col));
		context.write(celda, new DoubleWritable(rend));

		// Vecino de la "celda de abajo"
		celda = new CeldaWritable(new IntWritable(fila - 1), new IntWritable(col));
		context.write(celda, new DoubleWritable(rend));

		// Vecino de la celda del lateral izquierdo
		celda = new CeldaWritable(new IntWritable(fila), new IntWritable(col - 1));
		context.write(celda, new DoubleWritable(rend));

		// Vecino de la celda del lateral derecho
		celda = new CeldaWritable(new IntWritable(fila), new IntWritable(col + 1));
		context.write(celda, new DoubleWritable(rend));

	}

	private String[] datosXLinea(String linea) {
		linea = linea.replace("<", "").replace(">", "");
		return linea.split("	");
	}

}
