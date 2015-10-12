package com.hadoop.queryauthor;

public class BookAuthorMap {
private String author;
private String book;

/*
 * the input json is mapped to this class using Gson
 */
public BookAuthorMap(){}

public String getAuthor() {
	return author;
}

public String getBook() {
	return book;
}

}
