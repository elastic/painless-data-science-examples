# Explanation
This demos implementing finding frequent item sets and their support, which is a key ingredient of the [apriori](https://en.wikipedia.org/wiki/Apriori_algorithm#:~:text=Apriori%20is%20an%20algorithm%20for,sufficiently%20often%20in%20the%20database.) algorithm, using a scripted metric aggregation.

The basic idea behind this implementation is that an item set is only frequent if all its subsets are frequent. This means one doesn't need a brute force enumeration of all possible sets, but can instead grow frequent item sets by adding in a new unique item to the frequent item sets found already.

The aggregation query body is given in scripted_metric_frequent_sets.txt. This can be pasted into the console as follows:
```
GET apriori_demo/_search
// copy scripted_metric_frequent_sets.txt here
```

We also provide generator.py to generate some sample data with frequent item sets together with a background of infrequent random item sets. This assumes that the unique items are stored as individual fields, but in practice it may be more convenient to store them in a delimited string. It is easy to see how the string parsing functionality which we use in the script could be used to process data in that format instead.

# Comparing Painless and Python
Painless is closely related to Java and so if in doubt look at how a concept is expressed in Java to see how it will be expressed in Painless. It exposes a [subset](https://www.elastic.co/guide/en/elasticsearch/painless/master/painless-api-reference-shared.html) of Java's packages. It supports type erasure, but new variables have to be declared by prefixing with `def`. Whitespace is ignored and expressions need to be terminated with a `;`. Lambdas are supported and use the Java notation. Comparing some of the `HashMap` features this implementation uses to a Python `dictionary` we have `entrySet` -> `items`, `keySet` -> `keys`, `values` -> `values`, `getOrDefault` -> `get` with a `default` parameter value supplied, and `getKey` and `getValue` are used to access the key and value when iterating, as subscripting would be used to access these quantities from the Python tuple if it weren't unpacked.

This example also demos string manipulation via `StringJoiner` and `StringTokenizer`, manipulating collections using `Collections` and `Comparator`, and the `List`, `HashSet` and `ArrayList` types.

# The Anatomy of a Scripted Metric
For a discussion of the scripted metric see this [documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-scripted-metric-aggregation.html). It is worth highlighting that the script context are some special variables which allow one to interact with the index documents and maintain the program state. See in particular the "Scope of scripts" section in the linked documentation. Basically, documents are passed to the map_script as the `doc` variable. All program state is stored in either in the `state` variable or the `states` variable which exposes the return values of `combine_script` to the `reduce_script`.

# Tips and Tricks
This makes use of a very nice feature of the aggregation framework: the sampler aggregation. A key observation regarding frequent item sets is that they are also almost certain to be frequent in a sufficiently large random sample of the original data set. In fact, if an item has frequency f in the data set as a whole then in a random sample of size n its count will be [binomial distributed](https://en.wikipedia.org/wiki/Binomial_distribution) with parameters n and f (so large deviations have a square exponential decay). Here, we couple using the sampler aggregation with a random scorer, i.e.
```
"query": {
  "function_score": {
    "random_score": {}
  }
}
```
which means items will selected uniformly at random. The actual scripted aggregation to compute frequent items is nested below the sampler aggregation, i.e.
```
"aggs": {
  "random_sample": {
    "sampler": {
      "shard_size": 2000
    },
    "aggs": {
      "frequent_sets": {
        ...
```
This is particularly important because we need to ensure that the aggregation doesn't consume too much memory. We need to maintain a list of all unique item sets we found in the whole data set which could be as large as the index (if they are all unique). By sampling we impose an upper bound on the number of unique item sets which is the sample size rather than the index size and so can avoid a possible OOM.