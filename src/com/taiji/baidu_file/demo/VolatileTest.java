package com.taiji.baidu_file.demo;
/**
 *  study volatile 
 * @author andi
 *
 */
public class VolatileTest {
	
	 volatile boolean isExists;	
	public void tryExist(){
		if(isExists!=isExists){
			System.out.println("结束 啦");
			System.exit(0);
		}
	}
	
	public void swapValue(){
		 isExists=!isExists;
	}
	
	
	public static void main(String[] args){
		 final VolatileTest volatileTest = new VolatileTest();
		 Thread thread = new Thread(){
			 public void run(){
				 while(true){
					 System.out.println("------");
					 volatileTest.tryExist();
				 }
			 }
		 };
		 thread.start();
		 try {
			Thread.sleep(1);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 Thread thread2 = new Thread(){
			  public void run(){
				  while(true){
				      System.out.println("11111");
					  volatileTest.swapValue();
				  }
			  }
		 };
		 thread2.start();
		 
	}

}


