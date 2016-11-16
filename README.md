# Alpine Custom Operator Framework

Customers can now create Custom Operators to integrate with Alpine. This extensibilty framework opens up a new world of possibilities for data scientists and engineers.

- This framework allows one to create a wide variety of Custom Operators to be used within an Alpine analytics workflow.
- The Custom Operators can consume one or more tabular datasets and may produce a tabular dataset as output to following operators. 
- Custom Operators can be written in either Java or Scala.
- The Custom Operator framework supports Spark and Database operators. 
- Once Custom Operators are deployed to an Alpine instance, the end user will not need to read, write or understand any code in order to use the new Custom Operator.
- Specifically, users can easily write their own custom Spark code against their data within the Alpine framework.  

As a result, an entire team can share pre-existing libraries of analytics and have those incorporated into the Alpine visual workflows as simply another operator. This operator can be chosen and configured through the Alpine drag and drop interface very quickly. 

## What Version Should I Use?

There are different versions of the Custom Operator Framework that map to different versions of the Alpine software. For more information, see the page here: [How to Update Custom Operators With New SDK Changes](https://alpine.atlassian.net/wiki/display/KB/How+to+Update+Custom+Operators+With+New+SDK+Changes)

Alpine Version | Custom Operator SDK | Spark Version 
-------------- | --------------------------- | -------------
5.6.1          | Version 1.0, [Scaladoc](http://alpinenow.github.io/PluginSDK/1.0/api/), [Source](https://github.com/AlpineNow/PluginSDK/tree/release-1.0)   | 1.3.1 
5.7.1          | Version 1.3, [Scaladoc](http://alpinenow.github.io/PluginSDK/1.3/api/), [Source](https://github.com/AlpineNow/PluginSDK/tree/v1.3)  | 1.5.1
5.8            | Version 1.4.2, [Scaladoc](http://alpinenow.github.io/PluginSDK/1.4/api/), [Source](https://github.com/AlpineNow/PluginSDK/tree/v1.4)  | 1.5.1  
5.9            | Version 1.5.1, [Scaladoc](http://alpinenow.github.io/PluginSDK/1.5/api/), [Source](https://github.com/AlpineNow/PluginSDK/tree/v1.5)  | 1.5.1  
6.0            | Version 1.6, [Scaladoc](http://alpinenow.github.io/PluginSDK/1.6/api/), [Source](https://github.com/AlpineNow/PluginSDK/tree/v1.6)  | 1.5.1  
6.1            | [Version 1.7](https://github.com/AlpineNow/CustomPlugins/wiki/Release-Notes), [Scaladoc](http://alpinenow.github.io/PluginSDK/1.7/api/), [Source](https://github.com/AlpineNow/PluginSDK/tree/v1.7)  | 1.6.1
6.2            | Version 1.8, [Scaladoc](http://alpinenow.github.io/PluginSDK/1.8/api/), [Source](https://github.com/AlpineNow/PluginSDK/tree/v1.8)  | 1.6.1

## Resources

[How to Compile and Run the Sample Operators](https://alpine.atlassian.net/wiki/display/V5/How+To+Compile+and+Run+the+Sample+Operators)

[Building Your First Custom Operator in Scala](https://alpine.atlassian.net/wiki/display/V5/Building+Your+First+Custom+Operator+in+Scala)

[Building a Source Operator](https://alpine.atlassian.net/wiki/display/V5/Building+a+Source+Operator)

## Building on Windows

When building the samples, the tests may not run on Windows without some extra configuration. If you do not want to run the tests, you can build the sample operators in PluginExampleProject by running `mvn package -DskipTests`. This should build correctly and give you the JAR necessary to upload into Alpine.

If you want to be able to run the tests, know that Spark has extra requirements for reading and saving data in a Spark context on Windows. The linear regression model from MLlib (unlike the other Spark operators in PluginExampleProject) uses some Hadoop functionality to save intermediate results. Running Hadoop on Windows requires that you have a local Hadoop installation, the `HADOOP_HOME` variable correctly configured, and the winutils executable present on your machine. You may experience build errors if you are testing functionality using Spark Core or a Hive context. This is particularly true if you want to read and save data in a Windows environment. See [SPARK-2356](https://issues.apache.org/jira/browse/SPARK-2356) for more details.

[How to run Spark on Windows](http://nishutayaltech.blogspot.com/2015/04/how-to-run-apache-spark-on-windows7-in.html)
