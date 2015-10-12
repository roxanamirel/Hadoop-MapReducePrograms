package com.hadoop.queryauthor;


import java.util.List;
/*
 *  output class for the JSON output 
 *  GSON class converts this into a JSON representation 
 *  of this structure
 */
public class AuthorAndBooksReduce {

private String author;
private List<Book> books;


public AuthorAndBooksReduce(){}


public void setAuthor(String author) {
	this.author = author;
}


public void setBooks(List<Book> books) {
	this.books = books;
}


public String getAuthor() {
	return author;
}


public List<Book> getBooks() {
	return books;
}


}
