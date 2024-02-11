package com.itblueprints.sysagent.job;

import com.itblueprints.sysagent.job.Job;

public interface ScheduledJob extends Job {

    String getCron();
}
