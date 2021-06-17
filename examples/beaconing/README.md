# Explanation
This demos implementing detecting documents which arrive in a periodic pattern. This is useful because such regular signals are often a characteristic of beaconing malware.

Some other important features of such beaconing malware is that they display variation in frequency from time to time. This is typically due to (hidden) mode switches in the process behaviour, which result in step changes in frequency, or random jitter which is purposely applied to evade detection.

Here, we focus on analysing the distribution of document times in a time window. Using a time window naturally helps to deal with signals which are not consistently periodic or only active intermittently.

Since we need to avoid holding all documents in memory we can't analyse the time between documents directly. In fact, we need predictable maximum memory usage. To this end we choose to count documents falling in subintervals of the time window and check conditions on these counts which are satisfied if and only if the documents arrive in a periodic pattern. We also purposely drop leading and trailing empty buckets to deal with intermittently active beacons better.

If the bucket length is significantly longer than the period, we expect the variance in the count of documents per time bucket is much higher for randomly arriving documents than for periodically arriving documents. In particular, if the documents arrive according to a Poisson process we know that the variance of the count per bucket will be similar to the mean count per bucket. If we assert that the variance is significantly smaller than the mean we in effect detect signals which are much more regular than a Poisson process.

If the bucket length is significantly shorter than the period, we can actually observe the periodic pattern directly. In this case we can check for repeats which display a high autocovariance for offsets which are multiples of the period. We handle jitter directly by allowing the periodic pattern to shift slightly (by a fraction of the period) per repeat.

This approach has blind spots when:
1. The period is close to the bucket length
2. The window is too short to observe enough repeats

In the first case, beating between the regular pattern of documents and bucket endpoints muddies the waters. However, these need not be a problem in practice because the aggregation can be run on different window lengths and with different bucket lengths.