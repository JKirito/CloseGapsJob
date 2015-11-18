package MapRed;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import entities.CeldaWritable;

public class CloseGapsReducer extends Reducer<CeldaWritable, DoubleWritable, CeldaWritable, DoubleWritable> {

	public void reduce(CeldaWritable _key, Iterable<DoubleWritable> values, Context context) throws IOException,
			InterruptedException {

		int cantVecinos = 0;
		// process values
		double sumaRinde = 0.0;
		boolean hayRindeNeg = false;
		double rindeNeg = 0;
		for (DoubleWritable rend : values) {
			if (rend.get() < 0) {
				hayRindeNeg = true;
				rindeNeg = rend.get();
				break;
			}
			cantVecinos++;
			sumaRinde += rend.get();
		}

		if (hayRindeNeg) {
			context.write(_key, new DoubleWritable(rindeNeg * -1));
			return;
		}

		// si la cantidad de vecinos es al menos 3, "relleno"
		if (cantVecinos >= 3) {
			double mediaRinde = sumaRinde / cantVecinos;
			context.write(_key, new DoubleWritable(mediaRinde));
		}
	}
}
