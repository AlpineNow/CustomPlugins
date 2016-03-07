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
5.9            | Version 1.5, [Scaladoc](http://alpinenow.github.io/PluginSDK/1.5/api/), [Source](https://github.com/AlpineNow/PluginSDK/tree/v1.5)  | 1.5.1  

## Resources

[How to Compile and Run the Sample Operators](https://alpine.atlassian.net/wiki/display/V5/How+To+Compile+and+Run+the+Sample+Operators)

[Building Your First Custom Operator in Scala](https://alpine.atlassian.net/wiki/display/V5/Building+Your+First+Custom+Operator+in+Scala)

[Building a Source Operator](https://alpine.atlassian.net/wiki/display/V5/Building+a+Source+Operator)
