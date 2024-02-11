package com.itblueprints.sysagent.internal.scheduling;

import com.itblueprints.sysagent.*;
import com.itblueprints.sysagent.internal.cluster.NodeInfo;
import com.itblueprints.sysagent.internal.Config;
import com.itblueprints.sysagent.internal.SysAgentException;
import com.itblueprints.sysagent.internal.ThreadManager;
import com.itblueprints.sysagent.internal.Utils;
import com.itblueprints.sysagent.job.JobArguments;
import com.itblueprints.sysagent.job.ScheduledJob;
import com.itblueprints.sysagent.internal.job.JobExecutionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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
@Slf4j
public class SchedulerService {

    private final ConfigurableApplicationContext appContext;
    private final ThreadManager threadManager;
    private final MongoTemplate mongoTemplate;
    private final JobExecutionService jobExecutionService;
    private final Config config;

    final List<ScheduledJobItem> scheduledJobItems = new ArrayList<>();

    //------------------------------------------------------------
    public void onHeartBeat(NodeInfo nodeInfo, LocalDateTime now) {

        val heartBeatSecs = config.getHeartBeatSecs();

        for(val item : scheduledJobItems){
            //Look at jobs due since the last 3 heart beats. This should cover a previous manager failure
            val nextRunAt = item.cronExp.next(now.minusSeconds(heartBeatSecs * 3));
            val gap = ChronoUnit.SECONDS.between(nextRunAt, now);
            log.debug("gap: "+gap);

            var doRun = false;
            //Execute any missed jobs from another manager that has now failed
            if((gap < 0 && gap > heartBeatSecs * -3) && !item.lastRunAt.equals(nextRunAt)){
                val rec = mongoTemplate.findById(item.jobName, JobScheduleRecord.class);
                if(rec != null && rec.getLastRunAt().isBefore(nextRunAt)) doRun = true;
            }
            //Normal stuff
            else if(gap >= 0 && gap < heartBeatSecs) doRun = true;

            if(doRun)
            {
                threadManager.getExecutor().submit(() -> {
                    try {
                        Utils.sleepFor(gap < 0 ? 0 : (gap+1)*1000);
                        val args = new JobArguments();
                        args.put(SysAgentService.DataKeys.jobStartedAt, nextRunAt);
                        jobExecutionService.runJob(item.jobName, args);
                    }
                    catch (Exception e){
                        e.printStackTrace();
                        throw new SysAgentException("Error running scheduled job "+item.jobName, e);
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

        val heartBeatSecs = config.getHeartBeatSecs();
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
                throw new SysAgentException("Invalid CRON expression " + schJobBean.getCron(), e);
            }

            val item = new ScheduledJobItem();
            item.jobName = schJobBean.getName();
            item.cronExp = cronExp;
            item.lastRunAt = MIN_TIME;

            var rec = mongoTemplate.findById(schJobBean.getName(), JobScheduleRecord.class);
            if(rec==null){
                rec = new JobScheduleRecord();
                rec.setJobName(schJobBean.getName());
                rec.setLastRunAt(MIN_TIME);
                mongoTemplate.save(rec);
            }
            else {
                item.lastRunAt = rec.getLastRunAt();
            }
            scheduledJobItems.add(item);
        }
        log.debug("SchedulerService initialised");
    }

    private final LocalDateTime MIN_TIME = LocalDateTime.of(1900,1,1,0,0,0);

    //============================
    static class ScheduledJobItem{
        public String jobName;
        public CronExpression cronExp;
        public LocalDateTime lastRunAt = LocalDateTime.MIN;
    }

}
