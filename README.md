#Hadoop - Mapreduce
Extract a subset of german newspapers and news sites from the CDX Files in Hadoop Cluster 

## How to run

Compile and package the code using

    mvn package

Now you should have a .jar called `gen.sub-0.0.1-SNAPSHOT-job.jar`
in the directory `target` that contains all the classes necessary to run. Copy
the .jar to the cluster and execute using 

    hadoop jar gen.sub-0.0.1-SNAPSHOT-job.jar inputDirectory outputDirectory

Make sure that memory specification are set before you run,
as follows:

    export YARN_OPTS=-Xmx30G
    export HADOOP_CLIENT_OPTS="-Xmx10g"
    hadoop jar gen.sub-0.0.1-SNAPSHOT-job.jar har:/data/ia/derivatives/de/cdx/TB.har/ /user/souza/TB_subset_news
  

