package com.dw.TopK;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TopK {

	public static class ScoreBean implements WritableComparable<ScoreBean>{

		private Text grade;
		private DoubleWritable score;
		
		public ScoreBean() {

		}
		
		public ScoreBean(Text grade, DoubleWritable score) {
			set(grade, score);
		}
		
		public void set(Text grade, DoubleWritable score) {
			this.grade = grade;
			this.score = score;
		}
		
		public Text getGrade() {
			return grade;
		}

		public DoubleWritable getScore() {
			return score;
		}

		public void write(DataOutput out) throws IOException {
			out.writeUTF(grade.toString());
			out.writeDouble(score.get());
		}

		public void readFields(DataInput in) throws IOException {
			String readUTF = in.readUTF();
			double readDouble = in.readDouble();
			
			grade = new Text(readUTF);
			score = new DoubleWritable(readDouble);
		}

		public int compareTo(ScoreBean o) {
			int mini = grade.compareTo(o.getGrade());
			if(mini == 0) {
				mini = score.compareTo(o.getScore());
			}
			return mini;
		}
		
		public boolean equals(Object object) {
			if(object == null)
				return false;
			if(this == object)
				return true;
			if(object instanceof ScoreBean) {
				ScoreBean o = (ScoreBean) object;
				return grade.compareTo(o.getGrade()) == 0 && score.compareTo(o.getScore()) == 0;
			}else {
				return false;
			}
		}
		
		public String toString() {
			return grade.toString() + "\t" + score.get();
		}
	}
	
	public static class GradeGroupingComparator extends WritableComparator{
		protected GradeGroupingComparator() {
			super(ScoreBean.class, true);
			System.out.println("GroupingComparator---------------------------------");
		}
		
		public int compare(WritableComparable a, WritableComparable b) {
			ScoreBean abean = (ScoreBean) a;
			ScoreBean bbean = (ScoreBean) b;
			
			return abean.getGrade().compareTo(bbean.getGrade());
		}
	}
	
	public static class TopKMapper extends Mapper<LongWritable, Text, ScoreBean, DoubleWritable>{
		private final ScoreBean bean = new ScoreBean();
		
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split(",");
			
			bean.set(new Text(fields[1]), new DoubleWritable(Double.parseDouble(fields[2])));
			
			context.write(bean, new DoubleWritable(Double.parseDouble(fields[2])));
		}
	}
	
	public static class TopKReduce extends Reducer<ScoreBean, DoubleWritable, Text, DoubleWritable>{
		protected void reduce(ScoreBean key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			double maxS = 0;
			for(DoubleWritable val : values) {
				maxS = Math.max(maxS, val.get());
			}
			context.write(key.getGrade(), new DoubleWritable(maxS));
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration(true);
		String[] otherArgs = new String[2];
        otherArgs[0] = "hdfs://localhost:9000/user/dw/input/score.txt";
        String time = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        otherArgs[1] = "hdfs://localhost:9000/user/dw/mr-score-" + time;
 
        Job job = new Job(conf, "TopK");
        job.setJarByClass(TopK.class);
        job.setMapperClass(TopKMapper.class);
        
        // 分组函数
        job.setGroupingComparatorClass(GradeGroupingComparator.class);
 
        // Reducer类型
        job.setReducerClass(TopKReduce.class);
 
        // map输出Key的类型
        job.setMapOutputKeyClass(ScoreBean.class);
        // map输出Value的类型
        job.setMapOutputValueClass(DoubleWritable.class);
        // reduce输出Key的类型，是Text，因为使用的OutputFormatClass是TextOutputFormat
        job.setOutputKeyClass(Text.class);
        // reduce输出Value的类型
        job.setOutputValueClass(DoubleWritable.class);
 
        // 将输入的数据集分割成小数据块splites，同时提供一个RecordReder的实现。
        job.setInputFormatClass(TextInputFormat.class);
        // 提供一个RecordWriter的实现，负责数据输出。
        job.setOutputFormatClass(TextOutputFormat.class);
 
        FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
 
        // 提交job
        if (job.waitForCompletion(false)) {
            System.out.println("job ok !");
        } else {
            System.out.println("job error !");
        }
	}
}
