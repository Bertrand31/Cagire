# Damysos

![GitHub release](https://img.shields.io/github/release/Bertrand31/Damysos.svg)
![GitHub Release Date](https://img.shields.io/github/release-date/Bertrand31/Damysos.svg)
[![codecov](https://codecov.io/gh/Bertrand31/Damysos/branch/master/graph/badge.svg)](https://codecov.io/gh/Bertrand31/Damysos)
[![TravisCI](https://api.travis-ci.com/Bertrand31/Damysos.svg?branch=master)](https://travis-ci.com/Bertrand31/Damysos)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/b19c781500ef4434af54a6699892efcf)](https://www.codacy.com/app/bertrandjun/Damysos)
![GitHub issues](https://img.shields.io/github/issues/Bertrand31/Damysos.svg)
![GitHub](https://img.shields.io/github/license/Bertrand31/Damysos.svg)

- [Overview](#overview)
- [Performance](#performance)
- [Usage](#usage)
- [Caveats](#caveats)

## Overview

Damysos is an experiment stemming from the idea that tries could be used to store points in a
n-dimensional space in a way that would allow for fast querying of "neighboring" points.

In order to achieve this, we first have to turn each coordinate or every point into a base 4
number such that the more spacial proximity between two points, the more characters their
transformed coordinates share, and this, going from left to right.

For example, if point A's encoded abscissa coordinate is "111", point B's is "112" and point C's is
"100", we can tell that point A is closer to point B than it is of point C and point C is closer to
point A than it is of point B (along the foo aformentionned abscissa).

This way, in order to get the neighboring points of a coordinate, we only have to compute the
"trie path" for those coordinates, and descend the trie at the desired depth (the level of
precision, or "zoom"). Then, we take all the foo leaves below that point.

This would work well if we were storing monodimensional points. But, in our case, we chose to store
GPS coordinates, which are by nature bi-dimensional. To go from 1-dimensional coordinates to
n-dimensional coordinates while still maintaining the same level of performance, I had to come up
with a n-dimensional trie. It is basically a trie that, in each node, holds a n-dimensional array
representing each possible n-dimensional value of a n-dimensional path.

Because this may seem very abstract, we simply need to compare it to a normal trie: in a normal trie,
a "path" would be a word. For the word "foo", the path would be `List("f", "o", "o")`.
Now in a 2-dimensional trie, a path would look something like this: `List((1, 6), (4, 2), ...)`.
Notice we have tuples now, because each step of the path foo is 2-dimensional. I have replaced
characters with numbers simply because at this point, we're already far enough from the usual,
word-searching use-case of the usual Trie that we can stop pretending :)

The implementation of this n-dimensional trie is found in
[GeoTrie.scala](src/main/scala/damysos/GeoTrie.scala). However for the sake of simplicity and
because of the specificity of our use-case (GPS coordinates), GeoTrie is actually
a 2-dimensional trie. In the future, I might extract it into a separate project and really make it
n-dimensional (taking n as a constructor parameter) but as far as Damysos is concerned, there's no
point in doing that.

What's interesting in this approach, in my opinion, resides in the fact that nowhere in the code we
are actually commparing GPS coordinates, calculating distances etc. The data structure itself, in
this case a Trie, _is_ the logic.

## Performance

Here are the results of running the `PerfSpec` class on a laptop with an
_Intel Core i7-1065G7 CPU @ 1.30GHz_ CPU on a dataset of **23 435 958** points:
```text
============================
Profiling Damysos search:
Cold run        24 142 ns
Max hot         34 109 ns
Min hot         363 ns
Med hot         392 ns

============================
Profiling Linear search:
Cold run        189 494 799 ns
Max hot         177 992 346 ns
Min hot         149 712 474 ns
Med hot         150 693 588 ns

384422 times faster
```
As you can see, it is orders of magnitude faster than a linear search. This is because the Damysos
trie has a fixed height ; the amount of data we add to it has no influence over its depth or
structure. Retrieving neighbouring points takes constant time (`Θ(1)`), thus the more data we have,
the bigger the gap with the naïve, linear search.

The speed of that search, however, depends on the level of precision (or "zoom") you want to
achieve.  Although it may appear counter intuitive, a lower precision actually means a longer query
time. This is because, if we are using tries 10 levels deep and we ask for a precision of 5, then
we'll descend 5 levels of the trie (very fast, and tail-recursive) and then explore all the branches
below that point to get all the points underneath it (that's the slower part).
Hence, the lower the precision, the less we descend the trie before we start exploring all of its
sub-tries, so the more branches we'll have to explore from that point.

## Usage

First, create a Damysos instance. Then, feed it multiple `PointOfInterest` to add data to it:
```scala
import damysos.Damysos

val damysos = Damysos()
val paris = PointOfInterest("Paris" Coordinates(43.2D, -80.38333D))
val toulouse = PointOfInterest("Toulouse" Coordinates(43.60426D, 1.44367D))
val pointsOfInterest = Seq(paris, toulouse)
val bayonne = PointOfInterest("Bayonne", Coordinates(43.48333D, -1.48333D))
damysos ++ pointsOfInterest + bayonne
```
The `++` method accepts a `TraversableOnce` argument, it means you can feed it either a normal
`Collection` (like the `Seq` above) or a lazy `Iterator`:
```scala
import damysos.PointOfInterest

val data: Iterator[PointOfInterest] = PointOfInterest.loadFromCSV("cities_world.csv")
damysos ++ data // Lines will be pulled one by one from the CSV file to be added to the Damysos
```
From there, we can start querying our data structure:
```scala
damysos.contains(bayonne)

damysos.findSurrounding(paris)
```
Note that `findSurrounding` also takes an optional `precision` parameter to adjust the "zoom" level:
```scala
damysos.findSurrounding(paris, precision=4)
```
It also supports removing single element or a `TraversableOnce` of elements:
```scala
damysos - paris
damysos -- data
```
It also supports returning all of its contents as an `Array` and lastly, counting the number of
elements it contains:
```scala
damysos.toArray
damysos.size
```

## Caveats

Because of the way tries work and the encoding of coordinates, when we're nearing a "breakoff point"
of the base we have chosen, the trie won't "see" anything that is geographically close, but
which key is right after this breakoff point.

For example, the paths "333" and "400" have nothing in common as far as a trie is concerned and yet,
as base-4 numbers, they are numerically very close so the points they represent are also very close.

For this reason, Damysos will sometimes give incomplete results, and will be "blind" to everything
that is after of before the aforementionned "breakup points".

**This is why Damysos' goal is not to reliably provide exhaustive results, but rather return _some_
neighboring points, as quickly as possible.**

For this reason, it should be considered a probabilistic data structure: the surrounding points that
get returned are definitely neighbours of the coordinates you entered (no false positives), however,
 those results may very well be incomplete (false negatives).
