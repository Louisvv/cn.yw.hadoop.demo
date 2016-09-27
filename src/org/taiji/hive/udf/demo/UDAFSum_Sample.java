package org.taiji.hive.udf.demo;

import org.apache.hadoop.hive.ql.exec.NumericUDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.DoubleWritable;

public class UDAFSum_Sample extends NumericUDAF {
    public static class Evaluator implements UDAFEvaluator {
        private boolean mEmpty;
        private double mSum;
        
        
        public Evaluator() {
            super();
            init();
        }
        
        
        public void init() {
            mSum = 0;
            mEmpty = true;
        }
        
        
        public boolean iterate(DoubleWritable o) {
            if (o != null) {
                mSum += o.get();
                mEmpty = false;
            }
            return true;
        }
        
        
        public DoubleWritable terminatePartial() {
            // This is SQL standard - sum of zero items should be null.
            return mEmpty ? null : new DoubleWritable(mSum);
        }
        
        
        public boolean merge(DoubleWritable o) {
            if (o != null) {
                mSum += o.get();
                mEmpty = false;
            }
            return true;
        }
        
        public DoubleWritable terminate() {
            // This is SQL standard - sum of zero items should be null.
            return mEmpty ? null : new DoubleWritable(mSum);
        }
    }
}