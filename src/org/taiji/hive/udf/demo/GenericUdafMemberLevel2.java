package org.taiji.hive.udf.demo;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.commons.logging.Log;  
import org.apache.commons.logging.LogFactory;  
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;  
import org.apache.hadoop.hive.ql.metadata.HiveException;  
import org.apache.hadoop.hive.ql.parse.SemanticException;  
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;  
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;  
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver2;  
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;  
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;  
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;  
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;  
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;  
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;  
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;  
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;    
import org.apache.hadoop.util.StringUtils;  
  
public class GenericUdafMemberLevel2 extends AbstractGenericUDAFResolver {  
    private static final Log LOG = LogFactory  
            .getLog(GenericUdafMemberLevel2.class.getName());  
      
    @Override  
      public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)  
        throws SemanticException {  
          
        return new GenericUdafMeberLevelEvaluator();  
      }  
      
    public static class GenericUdafMeberLevelEvaluator extends GenericUDAFEvaluator {  
        private PrimitiveObjectInspector inputOI;  
        private PrimitiveObjectInspector inputOI2;  
        private PrimitiveObjectInspector outputOI;  
        private DoubleWritable result;  
  
        @Override  
        public ObjectInspector init(Mode m, ObjectInspector[] parameters)  
                throws HiveException {  
            super.init(m, parameters);  
              
            //init input  
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE){ //�������  
                LOG.info(" Mode:"+m.toString()+" result has init");  
                inputOI = (PrimitiveObjectInspector) parameters[0];  
                inputOI2 = (PrimitiveObjectInspector) parameters[1];  
//              result = new DoubleWritable(0);  
            }  
            //init output  
            if (m == Mode.PARTIAL2 || m == Mode.FINAL) {  
                outputOI = (PrimitiveObjectInspector) parameters[0];  
                result = new DoubleWritable(0);  
                return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;  
            }else{  
                result = new DoubleWritable(0);  
                return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;  
            }   
              
        }  
  
        /** class for storing count value. */  
        static class SumAgg implements AggregationBuffer {  
            boolean empty;  
            double value;  
        }  
  
        @Override  
        //�����µľۺϼ������Ҫ���ڴ棬�����洢mapper,combiner,reducer��������е�����ܺ͡�  
        //ʹ��buffer����ǰ���Ƚ����ڴ����ա���reset  
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {  
            SumAgg buffer = new SumAgg();  
            reset(buffer);  
            return buffer;  
        }  
  
        @Override  
        //����Ϊ0  
        //mapreduce֧��mapper��reducer�����ã�����Ϊ�˼��ݣ�Ҳ��Ҫ���ڴ�����á�  
        public void reset(AggregationBuffer agg) throws HiveException {  
            ((SumAgg) agg).value = 0.0;  
            ((SumAgg) agg).empty = true;  
        }  
  
        private boolean warned = false;  
        //����  
        //ֻҪ�ѱ��浱ǰ�͵Ķ���agg���ټ�������Ĳ������Ϳ����ˡ�  
        @Override  
        public void iterate(AggregationBuffer agg, Object[] parameters)  
                throws HiveException {  
            // parameters == null means the input table/split is empty  
            if (parameters == null) {  
                return;  
            }  
            try {  
                double flag = PrimitiveObjectInspectorUtils.getDouble(parameters[1], inputOI2);  
                if(flag > 1.0)   //��������  
                    merge(agg, parameters[0]);   //���ｫ�������ݷ���combiner���кϲ�  
              } catch (NumberFormatException e) {  
                if (!warned) {  
                  warned = true;  
                  LOG.warn(getClass().getSimpleName() + " "  
                      + StringUtils.stringifyException(e));  
                }  
              }  
  
        }  
  
        @Override  
        //����Ĳ������Ǿ���ľۺϲ�����  
        public void merge(AggregationBuffer agg, Object partial) {  
            if (partial != null) {  
                // ͨ��ObejctInspectorȡÿһ���ֶε�����  
                if (inputOI != null) {  
                    double p = PrimitiveObjectInspectorUtils.getDouble(partial,  
                            inputOI);  
                    LOG.info("add up 1:" + p);  
                    ((SumAgg) agg).value += p;  
                } else {  
                    double p = PrimitiveObjectInspectorUtils.getDouble(partial,  
                            outputOI);  
                    LOG.info("add up 2:" + p);  
                    ((SumAgg) agg).value += p;  
                }  
            }  
        }  
  
  
        @Override  
        public Object terminatePartial(AggregationBuffer agg) {  
                return terminate(agg);  
        }  
          
        @Override  
        public Object terminate(AggregationBuffer agg){  
            SumAgg myagg = (SumAgg) agg;  
            result.set(myagg.value);  
            return result;  
        }  
    }  
}  