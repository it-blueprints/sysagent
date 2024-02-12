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



![cluster](https://github.com/it-blueprints/sysagent/assets/22591521/5bf86c58-4010-4f24-887c-80265b8ac9d3)



## Getting started

Add dependency to POM

```
<dependency>
  <groupId>com.itblueprints</groupId>
  <artifactId>sysagent</artifactId>
  <version>0.0.4</version>
</dependency>
```

Annotate your main application class with ``@SysAgent``
```
@SpringBootApplication
@SysAgent
public class YourApplication {
  public static void main(String[] args) {
    SpringApplication.run(YourApplication.class, args);
  }
}
```

### A single step job
Next we need define a job. A job is a sequence of steps that are executed one after the other. 
Define your job class by implementing the ``Job`` interface
```
@Component
public class MyJob implements Job {

  @Override
  public JobPipeline getPipeline() {
    // Your job pipeline i.e. the sequence of steps goes here
    // Will populate this once we have defined the steps of the job
  }
}
```
Now we need to define the steps of a job. Steps are where you define your own implementation logic.

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

  @Autowired private MySimpleStep mySimpleStep;

  @Override
  public JobPipeline getPipeline() {
    //The pipeline with only one step
    return JobPipeline.create().firstStep(mySimpleStep); 
  }
  ...
```

### A multi-step job
Extending the above example, if you did want this to be a 2 step job, you can define another step like this 
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

  @Autowired private MySimpleStep mySimpleStep;

  @Autowired private MyNextSimpleStep myNextSimple; //the new step

  @Override
  public JobPipeline getPipeline() {
    //The pipeline with 2 steps
    return JobPipeline.create()
      .firstStep(mySimpleStep)
      .nextStep(myNextSimple); 
  }
 ...
```

### A scheduled jobs
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
```
