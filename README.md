# Cagire

This project aims to create a backend for a fulltext search service with autocomplete and real-time
results.

Through the use of a custom variation of a trie, it aims to search through thousands of documents
in a few miliseconds.

It supports two types of queries: the search of a whole word, which will return the matches for this
exact word, and the search of a partial word, or "prefix" (used to provide results as the user is
typing).

This "custom trie" works as follow: inside each leaf marking the end of a word, it also contains a
map of all the matches across all documents for that given word.

When we're searching for a prefix, we descend the trie along the prefix's characters, and then take
all the maps from all the leaves below that point. We then concatenate them.

The search through that trie is very fast, however since the API returns the whole lines where the
matches were found, we need to pull all those lines from the actual files on the disk (since we
don't want to keep all the data in memory). That part is the slowest because of the accesses to the
disk, and gets extremely slow when we're dealing with files of a few million lines.

To improve this, the files ingested are split into small chunks of 10 thousand lines. This way, we
later have to load a lot less data from the disk since we only open the useful chunks.

On my laptop (with an _Intel Core i7-1065G7 CPU @ 1.30GHz CPU_), it'll search through 31 million
words and return all the partial matches in 30ms.
It will search through the same amount of data and return exact word matches in 10ms.
