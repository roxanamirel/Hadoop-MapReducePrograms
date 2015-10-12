package com.hadoop.AuthorBook;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import com.google.gson.Gson;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 *  Modify this file to combine books from the same other into
 *  single JSON object.
 *  i.e. {"author": "Tobias Wells", "books": [{"book":"A die in the country"},{"book": "Dinky died"}]}
 *  Beaware that, this may work on anynumber of nodes!
 *
 */

public class CombineBooks {


	public static class BookMapper extends Mapper<Object, Text, Text, Text> {
		private Text authorT = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] lines = value.toString().split("\n");
			for (String line : lines) {
				Gson gson = new Gson();
				BookAuthorMap book = gson.fromJson(line, BookAuthorMap.class);
				authorT.set(book.getAuthor());
				context.write(authorT, new Text(book.getBook()));

			}

		}
	}
	public static class BookCombiner extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			AuthorAndBooksReduce ba = new AuthorAndBooksReduce();
			List<Book> books = new ArrayList<Book>();
			for (Text val : values) {
				Book book = new Book();
				book.setBook(val.toString());
				books.add(book);
			}
			ba.setBooks(books);
			ba.setAuthor(key.toString());
			Gson json = new Gson();

			context.write(key, new Text(json.toJson(ba)));
		}
	}
	public static class BookReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Gson gson = new Gson();
			List<Book> books = new ArrayList<Book>();
			AuthorAndBooksReduce result = new AuthorAndBooksReduce();
			
			for (Text val : values) {
				AuthorAndBooksReduce ba = gson.fromJson(val.toString(), AuthorAndBooksReduce.class);
				
				for(Book book:ba.getBooks()){
					books.add(book);
				}
			}
			result.setBooks(books);
			result.setAuthor(key.toString());
			

			context.write(new Text(gson.toJson(result)), new Text(""));
		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: CombineBooks <in> <out>");
			System.exit(2);
		}


		Job job = Job.getInstance(conf, "CombineBooks");
		job.setJarByClass(CombineBooks.class);
		job.setMapperClass(BookMapper.class);
	    job.setCombinerClass(BookCombiner.class);
		job.setReducerClass(BookReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
