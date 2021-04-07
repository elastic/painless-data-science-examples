# Overview
This is an internal repo where we can prototype interesting examples of using Painless to achieve adhoc data analysis with Elasticsearch. The idea is to have somewhere we can collaborate on developing examples before publishing more widely. An example must include a working Painless snippet and a Python test harness. The test harness must be able to create an index against which one can exercise the functionality and allow one to run it via the Python client. Ideally, any important implementation details should be discussed in the README of each example. It is fine to include multiple implementations of the same task to showcase different features of Painless. As a minimum discussions should include dangers and mitigations, such as using and how to avoid using too much memory in a scripted metric aggregation.

# Motivation
This grew out of a request to implement the [apriori](https://en.wikipedia.org/wiki/Apriori_algorithm#:~:text=Apriori%20is%20an%20algorithm%20for,sufficiently%20often%20in%20the%20database.) algorithm within the Elastic stack. It turns out that a scripted metric aggregation is able to do this, which is great. However, it is not straightforward to work out how to do this if 1. your primary programming language is not Java, 2. you use only the existing documentation. These examples are intended to provide a reference place where data scientist users of Elasticsearch can see pedagogical examples of using scripting to perform adhoc data analysis tasks with Elasticsearch. Aside from providing useful out-of-the-box functionality, the hope is to showcase how much one can achieve and help introduce this community to this useful functionality.

# Usage
Set up a virtual environment called `env`
```
python3 -m venv env
```
Activate it
```
source env/bin/activate
```
Install the required dependencies
```
pip3 install -r requirements.txt
```
Once you start an Elasticsearch instance, then each example includes a `generate` module which can be used to generate the demo data as, for example:
```
>>> import examples.apriori.generator as generator
>>> generator.generate_and_index_data(user_name='my_user', password='my_password')
```
where 'my_user' and 'my_password' are the user name and password for the Elasticsearch instance you've started. Each example also includes a `demo` module which can be run to see the output of the example as, for example:
```
>>> import examples.apriori.demo as demo
>>> demo.run(user_name='my_user', password='my_password')
```
For the apriori example you should see output like:
```
FREQUENT ITEM SETS DEMO...
FREQUENT_SETS(size=1)
   DIAMETER_PEER_GROUP_DOWN / support = 0.163
   DIAMETER_PEER_GROUP_DOWN_RX / support = 0.1385
   NO_PEER_GROUP_MEMBER_AVAILABLE / support = 0.309
   DIAMETER_PEER_GROUP_UP_TX / support = 0.1535
   PAD-Failure / support = 0.175
   NO_PROCESS_STATE / support = 0.1385
   NO_RESPONSE / support = 0.3305
   DIAMETER_PEER_GROUP_UP_RX / support = 0.145
   IP_REACHABLE / support = 0.5105
   RELAY_LINK_STATUS / support = 0.3675
   POM-Failure / support = 0.1765
   MISMATCH_REQUEST_RESPONSE / support = 0.1815
   vPAS-Failure / support = 0.1755
   PROCESS_STATE / support = 0.291
   IP_NOT_REACHABLE / support = 0.351
   DIAMETER_PEER_GROUP_DOWN_GX / support = 0.1405
FREQUENT_SETS(size=2)
   PAD-Failure PROCESS_STATE / support = 0.1445
   DIAMETER_PEER_GROUP_UP_TX POM-Failure / support = 0.1475
   MISMATCH_REQUEST_RESPONSE PAD-Failure / support = 0.1525
   ...
```