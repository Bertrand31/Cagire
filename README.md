# Cagire

This project aims to create a backend for a fulltext search service with autocomplete and real-time
results.

Through the use of an inverted index and a trie, it aims to search through thousands of documents
in under a second.

It supports two types of queries: the search of a whole word, looking up the inverted index
directly, and the search of a partial word (used to provide results as the user is typing).

The search of a partial word uses the trie to find out all the words stored that start with that
prefix. Once we have that list of words, we look up all of them in the inverted index.
