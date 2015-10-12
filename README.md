# Hadoop-MapReducePrograms
AuthorBook:
It parses an input author_book_tuple.txt", where each row is a tuple
formatted as JSON which contains an author and a book name.
The file in question contains a subset of \https://openlibrary.
org" data.
The program should do
the following: Given the input author-book tuples, map-reduce
program should procude a JSON object which contains all the
books from same author in a JSON array, i.e.
{"author": "Tobias Wells", "books":[{"book":"A die in the country"},{"book": "Dinky died"}]}


querryauthor:
The program should do the following:
Given the input author-book tuples, map-reduce program should
procude a JSON object which contains all the books from only
the queried author in a JSON array, i.e.
{"author": "Tobias Wells", "books":[{"book":"A die in the country"},{"book": "Dinky died"}]}
