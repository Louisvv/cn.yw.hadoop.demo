package org.taiji.hive.udf.demo;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class UpperUdf extends UDF{

		public final  Text evaluate(final Text t){
			if (t == null) {
				return null;
			}
				return new Text(t.toString().toUpperCase());
			
		}
}
