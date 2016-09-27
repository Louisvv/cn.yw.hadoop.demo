package org.taiji.hive.udf.demo;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;

@SuppressWarnings("unused")
public class AvgPriceUDAF extends UDAF {
	
	public static class UDAFAvgPriceState{
		private List<Integer> oldPriceList= new ArrayList<Integer>();
		private List<Integer> newPriceList= new ArrayList<Integer>();
	}
	
	public static class UDAFAvgPriceEvaluator implements UDAFEvaluator{
		UDAFAvgPriceState state;
		
		 public UDAFAvgPriceEvaluator() {  
	            super();  
	            state = new UDAFAvgPriceState();  
	            init();  
	        }  
		@Override
		public void init() {
			state.newPriceList=new ArrayList<Integer>();
			state.oldPriceList=new ArrayList<Integer>();
		}
		public boolean iterate(Integer avgPrice,Integer old){
			if (avgPrice !=null) {
				if(old == 1){
					state.oldPriceList.add(avgPrice);
				}
				else 
					state.newPriceList.add(avgPrice);	
				}
			
			return true;
		}
		
	}
	
			
			
			
		
		
	}


