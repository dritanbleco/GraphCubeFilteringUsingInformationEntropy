# GraphCubeFilteringUsingInformationEntropy


In order to run the code, we include a sample dataset that consists of two files (attributes.txt and LinkedData.txt zipped) 
The file attributes.txt contains information about the graph nodes attributes while the linkedData.txt denoted the edges of the graph.
The file RDDCubeMaterialization.scala includes the code for computing the graph cube. 
It declares two classes Link and Attributes that are used while parsing the input. These should be changed for other datasets. Another parameter to consider is the NumberOfAttributes that denotes the total number of attributes on  the graph (for both starting and ending nodes).

The file RDDEtropyFiltering.scala includes the logic for filtering the graph cuboids using entropy as is described in [1]. During execution, the code first filters out cuboids based on the external entropy threshold. For the remaining cuboids we calculate their internal entropy and perform filtering using the internal entropy rate. For other datasets, you should change the variable MAX_LENGTH (equals to 6 in our provided dataset) and the arrString that stores the name of the attributes.

In order to run the code using sbt you can use 

./bin/run-example RDDCubeMaterialization (or RDDEtropyFiltering) 
