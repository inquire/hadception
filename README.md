Hadception
===========

A nested map-reduce framework for the Apache Hadoop Project.

NOTE: 09.06.2012 - project goes into complete code revamp an upgrading internal job dispatch centralisation to a new version

Requirements:
--------------

* [*NIX] platform
* Java 1.6 JDK
* Hadoop Cluster (Single Node or Multiple Nodes)

Building:
--------------

Set configurations in Rakefile and ```rake```

Options you can invoke from ```rake```

```rake 

build      -- buildling the nmr framework.
clean      -- remove any temporary products.
cleanHDFS  -- cleaning hdfs locations.
clearjar   -- clear user code & nmr jars.
clobber    -- remove any generated file.
compile    -- compile user code with nmr framework.
deploy     -- deploy to cluster with default settings.
package    -- package code for cluster.
```




