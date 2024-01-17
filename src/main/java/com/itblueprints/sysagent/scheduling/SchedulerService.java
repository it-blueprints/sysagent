package com.itblueprints.sysagent.scheduling;

import com.itblueprints.sysagent.*;
import com.itblueprints.sysagent.cluster.ClusterService;
import com.itblueprints.sysagent.cluster.NodeInfo;
import com.itblueprints.sysagent.job.JobService;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

@Component
@RequiredArgsConstructor
public class SchedulerService {

    private final ConfigurableApplicationContext appContext;
    private final ThreadManager threadManager;
    private final MongoTemplate mongoTemplate;
    private final JobService jobService;

    private final List<ScheduledJobItem> scheduledJobItems = new ArrayList<>();

    //--------------------------------------------
    public void onHeartBeat(NodeInfo nodeInfo) {

        val now = LocalDateTime.now();
        for(val item : scheduledJobItems){
            //Look at jobs due since the last 3 heart beats. This should cover a previous manager failure
            val nextRunAt = item.cronExp.next(now.minusSeconds(ClusterService.HEARTBEAT_SECS * 3));
            val gap = ChronoUnit.SECONDS.between(LocalDateTime.now(), nextRunAt);
            Dbg.p("gap", gap);

            var doRun = false;
            //Execute any missed jobs from another manager that has now failed
            if(gap < 0 && !item.lastRunAt.equals(nextRunAt)){
                val rec = mongoTemplate.findById(item.jobName, JobScheduleRecord.class);
                if(rec != null && rec.getLastRunAt().isBefore(nextRunAt)) doRun = true;
            }
            //Normal stuff
            else if(gap >= 0 && gap < ClusterService.HEARTBEAT_SECS) doRun = true;

            if(doRun)
            {
                threadManager.submit(() -> {
                    try {
                        Utils.sleepFor(gap < 0 ? 0 : (gap+1)*1000);
                        val args = new Arguments();
                        args.put(runAt, nextRunAt);
                        Dbg.p("Here 1");
                        jobService.runJob(item.jobName, args, nodeInfo);
                    }
                    catch (Exception e){
                        e.printStackTrace();
                        throw new SystemAgentException("Error running scheduled job "+item.jobName, e);
                    }
                });
                val rec = mongoTemplate.findById(item.jobName, JobScheduleRecord.class);
                rec.setLastRunAt(nextRunAt);
                item.lastRunAt = nextRunAt;
                mongoTemplate.save(rec);
            }
        }
    }

    //-------------------------------------------------
    public void initialise(NodeInfo nodeInfo){

        val beanFactory = appContext.getBeanFactory();
        val scheduledJobBeanNames = beanFactory.getBeanNamesForType(ScheduledJob.class);

        for(val beanName: scheduledJobBeanNames){
            val schJobBean = beanFactory.getBean(beanName, ScheduledJob.class);
            //Granularity is minutes
            val cron = "0 "+schJobBean.getCron();
            CronExpression cronExp;
            try {
                cronExp = CronExpression.parse(cron);
            }
            catch (IllegalArgumentException e) {
                throw new SystemAgentException("Invalid CRON expression " + schJobBean.getCron(), e);
            }

            val item = new ScheduledJobItem();
            item.jobName = schJobBean.getName();
            item.cronExp = cronExp;
            val lastRunAt = nodeInfo.timeNow.minusSeconds(ClusterService.HEARTBEAT_SECS);
            item.lastRunAt = lastRunAt;

            var rec = mongoTemplate.findById(schJobBean.getName(), JobScheduleRecord.class);
            if(rec==null){
                rec = new JobScheduleRecord();
                rec.setJobName(schJobBean.getName());
                rec.setLastRunAt(lastRunAt);
                mongoTemplate.save(rec);
            }
            scheduledJobItems.add(item);
        }
    }

    //============================
    static class ScheduledJobItem{
        public String jobName;
        public CronExpression cronExp;
        public LocalDateTime lastRunAt;
    }

    public static final String runAt = "_runAt";
}
