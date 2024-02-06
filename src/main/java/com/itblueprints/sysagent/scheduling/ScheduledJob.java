package com.itblueprints.sysagent.scheduling;

import com.itblueprints.sysagent.job.Job;

public interface ScheduledJob extends Job {

    String getCron();
}
