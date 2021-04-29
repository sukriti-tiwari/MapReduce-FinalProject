
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class Mapreduce2 {
	
	public  static class Mymapper extends Mapper<LongWritable,Text,Text,Text>{
	
		@Override
		protected void map(LongWritable offset, Text line,Context context)
				throws IOException, InterruptedException {
			 String curr=line.toString();
			String text[]=curr.split(",");
			int consumption_at_month=Integer.parseInt(text[3]);
			int consumption_at_nextmonth=Integer.parseInt(text[4]);
      String Cluster1=getCluster(text,consumption_at_month);
      String Cluster2=getCluster(text,consumption_at_nextmonth);
      double percent=(double)(consumption_at_nextmonth-consumption_at_month)/consumption_at_month;
      percent=100.0*percent;   
           context.write(new Text(Double.toString(percent)+","+Cluster1+","+Cluster2),new Text(text[0]));
          
 }
	

		private String getCluster(String text[],int load) {
			String Cluster;
			if(text[0].charAt(0)=='C')
			{
				if(load<=1500)
					Cluster="1_Commerical_Cluster(0 to 1500 Kwh )";
				else if(load<=3000)
					Cluster="2_Commerical_Cluster2(1500 to 3500 Kwh)";
				else if(load<=5000)
					Cluster="3_Commerical_Cluster3(3501 to 5000 Kwh)";
				else
					Cluster="4_Commerical_Cluster3(greater than 5000 Kwh)";
			}	
			else
			{
				if(load<=100)
					Cluster="1_Home_Cluster(0 to 100 Kwh)";
				else if(load<=500)
					Cluster="2_Home_Cluster(101 to 500 Kwh)";
				else if(load<=750)
					Cluster="3_Home_Cluster(500 to 750  Kwh)";
				else
					Cluster="4_Home_Cluster(greater than 750 Kwh)";
				
		}
			
			return Cluster;
		
		}
			 
			
		 }
public  static class Mymapperforfinal extends Mapper<LongWritable,Text,Text,DoubleWritable>{
	
	@Override
	protected void map(LongWritable offset, Text line,Context context)
			throws IOException, InterruptedException {
        String curr=line.toString();
        String text[]=curr.split(",");
		int consumption_at_month=Integer.parseInt(text[3]);
		int consumption_at_nextmonth=Integer.parseInt(text[4]);
  double percent=(double)(consumption_at_nextmonth-consumption_at_month)/consumption_at_month;
  percent=100.0*percent;
       
       context.write(new Text(text[0]),new DoubleWritable(percent));
      

	}
}	
	  public static class TypePartitioner extends Partitioner < Text, Text >
	   {
	      @Override
	      public int getPartition(Text key, Text value, int numReduceTasks)
	      {
	        
	         
	         if(numReduceTasks == 0)
	         {
	            return 0;
	         }
	       
	         else if(value.charAt(0)=='C')
	         {
	            return 0;
	         }
	         else
	         {
	            return 1 % numReduceTasks;
	         }
	      }
	   }
	
	public static class myReducerforclassifier1 extends Reducer<Text,Text,Text,Text> {
     
      Context cont;
		@Override
		protected void reduce(Text val, Iterable<Text> arr,Context cxt)
				throws IOException, InterruptedException {
			
			
			for( Text text:arr)
			{
				 String curr=val.toString();
				String content[]=curr.split(",");
				cxt.write(text,new Text(content[1]));
				
				
	        }
		}
		
		}
	public static class myReducerforclassifier2 extends Reducer<Text,Text,Text,Text> {
	      
	     
			@Override
			protected void reduce(Text val, Iterable<Text> arr,Context cxt)
					throws IOException, InterruptedException {
				
				
				for( Text text:arr)
				{
					 String curr=val.toString();
					String content[]=curr.split(",");
					cxt.write(text,new Text(content[2]));
					
					
		        }
			}
			
			}
	public static class myReducerforFinal extends Reducer<Text,Text,Text,IntWritable> {
	    
		int[] counters = new int[] {0,0,0,0,0,0,0,0,0,0,0};
		
			@Override
			protected void reduce(Text val, Iterable<Text> arr,Context cxt)
					throws IOException, InterruptedException {
				
				
				for( Text d:arr)
				{
				
					 String curr=val.toString();
						String content[]=curr.split(",");
				
					double change=Double.parseDouble(content[0]);
					if(change<0)
					{
						if(change>=-10)
							counters[0]++;
						else if(change>=-20)
							counters[1]++;
						else if(change>=-30)
							counters[2]++;
						else if(change>=-40)
							counters[3]++;
						else
							counters[4]++;
						
					}
					else if(change==0)
					{
						counters[5]++;
					}
					else
					{
						if(change<=10)
							counters[6]++;
						else if(change<=20)
							counters[7]++;
						else if(change<=30)
							counters[8]++;
						else if(change<=40)
							counters[9]++;
						else
							counters[10]++;
						
					}
					
				
					
		        }
			}
			
			
			protected void cleanup(Context context)
					throws IOException, InterruptedException {		
	
			context.write(new Text("decrease greater than 40%"),new IntWritable(counters[4]));
			context.write(new Text("decrease 30 to 40%"),new IntWritable(counters[3]));
			context.write(new Text("decrease 20 to 30%"),new IntWritable(counters[2]));
			context.write(new Text("decrease 20 to 10%"),new IntWritable(counters[1]));
			context.write(new Text("decrease 10 to 0%"),new IntWritable(counters[0]));
			context.write(new Text("No change"),new IntWritable(counters[5]));
			context.write(new Text("increase 0 to 10%"),new IntWritable(counters[6]));
			context.write(new Text("increase 10 to 20%"),new IntWritable(counters[7]));
			context.write(new Text("increase 20 to 30%"),new IntWritable(counters[8]));
			context.write(new Text("increase 30 to 40%"),new IntWritable(counters[9]));
			context.write(new Text("increase greater than 40%"),new IntWritable(counters[10]));
			
			
			
			}
	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
        Configuration conf=new Configuration();
	Job jobsplit1=new Job(conf,"Classifier1");
	Job jobsplit2=new Job(conf,"Classifier2");
		Job jobresult=new Job(conf,"Analysis");
		
		
	jobsplit1.setJarByClass(Mapreduce2.class);
	jobsplit2.setJarByClass(Mapreduce2.class);
		jobresult.setJarByClass(Mapreduce2.class);
		
  	jobsplit1.setMapperClass(Mymapper.class);
 	jobsplit2.setMapperClass(Mymapper.class);
    	jobresult.setMapperClass(Mymapper.class);
		
	jobsplit1.setReducerClass(myReducerforclassifier1.class);
	jobsplit2.setReducerClass(myReducerforclassifier2.class);
     jobresult.setReducerClass(myReducerforFinal.class);
     	
	jobsplit1.setMapOutputKeyClass(Text.class);
	jobsplit1.setMapOutputValueClass(Text.class);
	jobsplit2.setMapOutputKeyClass(Text.class);
	jobsplit2.setMapOutputValueClass(Text.class);
		

		jobresult.setMapOutputKeyClass(Text.class);
		jobresult.setMapOutputValueClass(Text.class);
		jobsplit1.setPartitionerClass(TypePartitioner.class);
		jobsplit2.setPartitionerClass(TypePartitioner.class);
		jobresult.setPartitionerClass(TypePartitioner.class);
	    jobresult.setNumReduceTasks(2);
    jobsplit1.setNumReduceTasks(2);
	    jobsplit2.setNumReduceTasks(2);
		jobsplit1.setOutputKeyClass(Text.class);
	    jobsplit1.setOutputValueClass(Text.class);
		jobsplit2.setOutputKeyClass(Text.class);
	    jobsplit2.setOutputValueClass(Text.class);
		jobresult.setOutputKeyClass(Text.class);
	    jobresult.setOutputValueClass(Text.class);
		 FileInputFormat.addInputPath(jobsplit1,new Path(args[0]));
		 FileInputFormat.addInputPath(jobsplit2,new Path(args[0]));
		 FileInputFormat.addInputPath(jobresult,new Path(args[0]));
		 
	     FileOutputFormat.setOutputPath(jobsplit1,new Path(args[1]));
	     FileOutputFormat.setOutputPath(jobsplit2,new Path(args[2]));
	     FileOutputFormat.setOutputPath(jobresult,new Path(args[3]));
	     
	     
	     System.out.println("/n/nRunning classification algorithm.../n/n");
	     System.exit (jobsplit2.waitForCompletion(true)&&jobsplit1.waitForCompletion(true)&&jobresult.waitForCompletion(true)? 0 : 1);	
}

}



