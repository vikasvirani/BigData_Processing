s3715555 - Vikas Virani


Instructions to Run the code:-

1) The program contains three command line arguments in total. First argument is the path to the data file that is being used, this is required.

2) Other two arguments are optional & descibed as below,

====>       2nd Argument is the number of Clusters that need to be initialised randomly. If nothing is provided, it will randomly initialise 10 clusters from dataset.

====>       3rd Argument is the fraction of dataset that you want to consider in processing, i.e. if the third argument is "100" & dataset size is 100000 rows then 
	 it will consider 100th fraction of dataset, i.e. 1000 rows, as datapoints. This is done to avaoid the task of uploading JAR every time changes are made 
	 to test the code on different dataset sizes. Use this fraction to test on a part of dataset instead of whole dataset.

3) run the code as regular & it will start performing each iteration of map reduce till convergence.


I've added a dependency in pom.xml for ArrayWritable class, so it is advisable to use whole project when running a job instead of just source files, which can give
pom.xml dependency error. 