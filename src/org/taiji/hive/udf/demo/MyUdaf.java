package org.taiji.hive.udf.demo;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hive.com.esotericsoftware.minlog.Log;

public class MyUdaf extends AbstractGenericUDAFResolver {

	// static final Log LOG = LogFactory.
	
	@Override
	public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info)
			throws SemanticException {
		
		return super.getEvaluator(info);
	}

	
}
