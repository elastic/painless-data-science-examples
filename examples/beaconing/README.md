# Explanation
This demos detecting documents which arrive in a periodic pattern with a scripted metric aggregation. This is useful because such regular signals are often a characteristic of beaconing malware.

Some other important features of beaconing malware is that they display variation in frequency from time to time. This is typically due to (hidden) mode switches in the process behaviour, which result in step changes in frequency, or random jitter which is purposely applied to evade detection.

Here, we focus on analysing the distribution of document times in a time window. Using a time window naturally helps to deal with signals which are not consistently periodic or only active intermittently.

Since we need to avoid holding all documents in memory we can't analyse the time between documents directly. In fact, we need predictable maximum memory usage for any script we deploy on an Elasticsearch cluster. To this end we choose to count documents falling in subintervals (or buckets) of the time window and check conditions on these counts which are satisfied if and only if the documents arrive in a periodic pattern.

If the bucket length is significantly longer than the period, we expect the variance in the count of documents per bucket is much higher for randomly arriving documents than for periodically arriving documents. In particular, if the documents arrive according to a Poisson process we know that the variance of the count per bucket will be similar to the mean count per bucket. If we assert that the variance is significantly smaller than the mean we, in effect, detect signals which are much more regular than a Poisson process.

If the bucket length is shorter than the period, we can actually observe the periodic pattern directly. In this case we can use the fact that buckt counts for periodically arriving documents will have high autocovariance for offsets which are multiples of the period. We handle jitter directly by allowing the periodic pattern to shift slightly (by a fraction of the period) per repeat.

This approach has blind spots when:
1. The period is close to the bucket length
2. The window is too short to observe enough repeats

In the first case, beating between the regular pattern of documents and bucket endpoints can muddy the waters. However, these need not be a problem in practice because the aggregation can be run on different window lengths and with different bucket lengths.

# Comparing Painless and Python
This example demos Painless [functions](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-functions.html). Syntatically, these are very similar to Python functions. You simply need to declare them in a script body before they are used.

It is also worth digging a little deeper into typing in Painless in this context. Unlike Python, Painless requires that types are declared. However, one is always free to declare types with def which is a type placeholder. For example, the mean function could be declared as either `float mean(int a, int b, int stride, float[] array)` or `def mean(def a, def b, def stride, def array)`, or in fact any mixture of the two. Functions which are declared with type placeholders for arguments can be called with any type which supports the operations the function requires. For example, the mean function expects the `array` parameter to support element access via the [] operator. When used with `def`, Painless is weakly typed and misuse generates a runtime exception. For example, if we were to erroneously call `def mean(def a, def b, def stride, def array)` as `mean(0, 1, 1, 16);` then we would get a script exception, something like "Attempting to address a non-array-like type [java.lang.Integer] as an array" whilst it is running. If instead we had declared the types explicitly, as in `float mean(int a, int b, int stride, float[] array)`, we would get a compile error when trying to run the scripted aggregation. These are often more informative.

# Tips and Tricks
The scripted metric aggregation supports a "params" section. These are available to all other scripts. In the context of complex scripts think of this section exactly like program command line arguments. In this example, we pulled out key parameters, such as thresholds for test statistics at which to classify signals as beaconing, into the params section. This is generally good practice for the obvious reasons: it provides a single point of definition for important parameters, so ensuring all uses are consistent, and it provides a simple reliable experience when editing parameters of a scripted metric.