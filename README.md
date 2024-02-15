# SysAgent

A common scenario these days is to run a SpringBoot application in a cluster (e.g. on AWS ECS) with MongoDB as the database. 
However, this setup poses a problem if you need to run a batch job. This is when a large number of records need to be
processed automatically within a certain timeframe. One could run the job on one node in the cluster, but then that specific
node needs to be temporarily scaled up in terms of processing capacity. This is tricky to say the least.

SysAgent is a Spring library that turns a cluster of SpringBoot nodes into a simple batch processing setup. It does
this by designating a Manager node, which splits up the work into paritions and each partition is then processed by
one of the nodes in the cluster. Obviously, this only works when the work can be partitioned. But in majority of the
cases this is not a problem. There usually exists some scheme by which records in the batch can be assigned to different
partitions.

SysAgent also  has a built it scheduler for scheduled jobs. The manager node is responsible for executing the job at the
scheduled time. In the event the current manager goes down, the next manager node ensures that the job is triggered.

Here is a typical setup

![cluster](https://github.com/it-blueprints/sysagent/blob/main/docs/img/sysagent-cluster.drawio.png)

One of the nodes in the cluster becomes the Manager. It then carries out certain duties such as starting scheduled jobs
reclaiming work from dead nodes etc. The Manager Nodes along with the other nodes makes up the Worker pool. The job of
the Worker is to basically execute system activities of your choice. All this is coordinated via the database

### The domain model
Here are the important concepts of SysAgent

![cluster](https://github.com/it-blueprints/sysagent/blob/main/docs/img/sysagent-domain.drawio.png)

All of these are Java interfaces. In your code you would provide implementions of these. The only exception to this is the ``Step``
interface, which is an abstract concept and should not be used directly

|Entity                | Description|
|----------------------|--------------------------------------------------------------------------------------------|
|Job                   |An activity that needs to be carried out by the system. Acts as a container for one or more steps that need to be executed|
|ScheduledJob          |A Job that needs to start executing at a predefined time                                    |
|Step                  |A step in the job. Contains the actual logic of what needs to be done. You implement one of the subtypes of Step|
|SimpleStep            |A step for a simple action.You implement the ``run()`` method which contains the business logic. The step is executed on one node in the cluster|
|PartitionedStep       |Same as simple step, except you have to also  implement the ``getPartitions()`` method. This allows data to be divided into partitions and distributed among different worker nodes to execute. Each worker node processes data from one partition|
|BatchStep             |A step that provides a setup such that items can be processed using multiple threads. The step is executed on one node in the cluster|
|PartitionedBatchStep  |A combination of PartitionedStep and BatchStep. That is, the work itself is partitioned and distributed among worker nodes. Each node processes one partition and the items in the partition are processed using multiple threads|


## Getting started

Ensure that your project's application.properties is correctly setup to use MongoDB. For example it has something like this
```
spring.data.mongodb.uri=mongodb://localhost:27017/mydb
```

Next, we add this to the POM

```
<dependency>
  <groupId>com.itblueprints</groupId>
  <artifactId>sysagent</artifactId>
  <version>0.0.5</version>
</dependency>
```

Then annotate your main application class with ``@SysAgent``
```
@SpringBootApplication
@SysAgent
public class MyApplication {
  public static void main(String[] args) {
    SpringApplication.run(MyApplication.class, args);
  }
}
```

### A simple job
Next we need to define a job bean. A job is a sequence of steps that are executed one after the other. 
Define your job class by implementing the ``Job`` interface
```
@Component
public class MyJob implements Job {

  @Override
  public JobPipeline getPipeline() {
    // Your job pipeline i.e. the sequence of steps goes here (see below)
  }
}
```
Now we need to define the steps of a job. Steps beans are where you define your own implementation logic.

Let's start with the simplest scenario where only one action needs to be carried out i.e. the job only has a single step and
that step has some logic that needs to be run. In that case define a step by implementing the ``SimpleStep`` interface
```
@Component
public class MySimpleStep implements SimpleStep {

  @Override
  public void run(StepContext context) {
    //Implement your step logic here      
  }
}
```

Once this step has been defined, we need to go back to the job and define its pipeline like so
```
@Component
public class MyJob implements Job {

  @Autowired private MySimpleStep step1;

  @Override
  public JobPipeline getPipeline() {
    //The pipeline with only one step
    return JobPipeline.create().firstStep(step1); 
  }
  ...
```

### A multi-step job
Extending the above example, if you did want this to be a 2 step job, you can define another step bean like this 
```
@Component
public class MyNextSimpleStep implements SimpleStep {
  ...
}
```
and then set up the pipeline in ``MyJob`` like this to include the second step
```
@Component
public class MyJob implements Job {

  @Autowired private MySimpleStep step1;

  @Autowired private MyNextSimpleStep step2; //the second step

  @Override
  public JobPipeline getPipeline() {
    //The pipeline with 2 steps
    return JobPipeline.create()
      .firstStep(step1)
      .nextStep(step2); 
  }
 ...
```
### A scheduled job
If your job needs to be scheduled to run at a specific time of the day, you can specify a CRON expression for it. All you need is for your job
to implement the ``ScheduledJob`` interface. Now you are expected to implement the ``getCron()`` method like so
```
public class MyJob implements ScheduledJob {

  @Override
  public JobPipeline getPipeline() {
    ...
  }

  @Override
  public String getCron() {
    //This job will be triggered at midnight everyday.
    return "0 0 * * *";
  }
}
```

### Partitioned steps
So far we have come across the ``SimpleStep``, which just has a ``run()`` method that you need to override. And this step gets executed on one of the
worker nodes. However if you data set can be partitioned i.e. it has one or more attributes allows it to be grouped into smaller sets, then we can make 
use of a ``ParitionedStep``. 

To illustrate how it works, let us say you are processing a 1000 records from the database. And say there is an attribute/column called 'category' which 
has one of the following values - A, B and C. Further, the number of records in each category are in the same ballpark, say around 300. Assuming you have 
3 nodes in your cluster it would make sense to process each category on a different node, thus utilising all the nodes to complete the job faster. 

A ``PartitionedStep`` along with the ``run()`` method has one for method you need to override - ``getPartitions()``. And your implmentation will be along
the lines of
```
@Override
public List<Partition> getPartitions(JobArguments jobArgs) {
  return List.of(
    Partition.from(Map.of("category", "A")),
    Partition.from(Map.of("category", "B")),
    Partition.from(Map.of("category", "C"))
  );
}
```
A Parition object is just a Map<String, Object> which holds all partition keys for a partition. When SysAgent comes to execute this step it first invokes
``getPartitions()`` to get the list of Partition objects and then puts them as independent tasks in the database. When a worker node finds this task
it it starts executing it by invoking the ``run()`` method. A StepContext object is passed in as an argument to ``run()``. This object (which is also a 
Map<String, Object>() contains information for a partition. You can then use this information to decide which section of the data set to process. Typically
the pseudocode of your ``run()`` method will be as follows -
```
cat = context.getString("category")
get all records from the database where record.category = cat
process these records
``` 

### Batched steps


