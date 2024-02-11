package com.itblueprints.sysagent.internal.repository;

import com.itblueprints.sysagent.internal.ExecutionStatus;
import com.itblueprints.sysagent.internal.cluster.ManagerNodeRecord;
import com.itblueprints.sysagent.internal.cluster.BaseNodeRecord;
import com.itblueprints.sysagent.internal.cluster.NodeRecord;
import com.itblueprints.sysagent.internal.job.JobRecord;
import com.itblueprints.sysagent.internal.scheduling.JobScheduleRecord;
import com.itblueprints.sysagent.internal.step.StepRecord;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
public class MongoRecordRepository implements RecordRepository {

    private final MongoTemplate mongoTemplate;

    //****************** Node Records ********************

    @Override
    public <T extends BaseNodeRecord> T save(T nodeRecord) {
        return mongoTemplate.save(nodeRecord);
    }

    //--------------------------------------
    @Override
    public ManagerNodeRecord getManagerNodeRecord() {

        val query = new Query();
        query.addCriteria(Criteria
                .where("unique").is(true));
        return mongoTemplate.findOne(query, ManagerNodeRecord.class);
    }

    //--------------------------------------
    @Override
    public NodeRecord getNodeRecordById(String id) {
        val query = new Query();
        query.addCriteria(Criteria
                .where("id").is(id));
        return mongoTemplate.findOne(query, NodeRecord.class);
    }

    //--------------------------------------
    @Override
    public ManagerNodeRecord tryGetLockedManagerNodeRecord() {
        val mgrQuery = new Query();
        mgrQuery.addCriteria(Criteria
                .where("unique").is(true)
                .and("locked").is(false));
        val update = new Update();
        update.set("locked", true);
        val mgrNR = mongoTemplate.findAndModify(mgrQuery, update, ManagerNodeRecord.class);
        if(mgrNR!=null) mgrNR.setLocked(true);
        return mgrNR;
    }

    //--------------------------------------
    @Override
    public List<NodeRecord> getRecordsForOtherNodes(String thisNodeId) {
        val allNRs = mongoTemplate.findAll(NodeRecord.class);
        return allNRs.stream()
                .filter(nr -> ! nr.getId().equals(thisNodeId))
                .toList();
    }

    //--------------------------------------
    @Override
    public void delete(BaseNodeRecord nodeRecord) {
        mongoTemplate.remove(nodeRecord);
    }

    //****************** Job Records ********************

    @Override
    public JobRecord save(JobRecord jobRecord) {
        return mongoTemplate.save(jobRecord);
    }

    //--------------------------------------
    @Override
    public List<JobRecord> getRunningJobRecords() {
        val executingJobsQuery = new Query();
        executingJobsQuery.addCriteria(Criteria
                .where("status").is(ExecutionStatus.RUNNING));
        return mongoTemplate.find(executingJobsQuery, JobRecord.class);
    }

    //--------------------------------------
    @Override
    public void initialise() {
        mongoTemplate.indexOps(JobRecord.class)
                .ensureIndex(new Index()
                        .on("jobRecordId", Sort.Direction.ASC)
                        .on("stepName", Sort.Direction.ASC)
                        .on("status", Sort.Direction.ASC));
    }

    //--------------------------------------
    @Override
    public JobRecord getFailedJobRecordOfJob(String jobName) {
        val query = new Query();
        query.addCriteria(Criteria
                .where("jobName").is(jobName)
                .and("status").is(ExecutionStatus.FAILED));
        return mongoTemplate.findOne(query, JobRecord.class);
    }

    //****************** Step Records ********************

    @Override
    public StepRecord save(StepRecord stepRecord) {
        return mongoTemplate.save(stepRecord);
    }

    //--------------------------------------
    @Override
    public List<StepRecord> getStepsRecordsForStepOfJob(String jobRecordId, String stepName) {
        val query = new Query();
        query.addCriteria(Criteria
                .where("jobRecordId").is(jobRecordId)
                .and("stepName").is(stepName)
        );
        return mongoTemplate.find(query, StepRecord.class);
    }

    //--------------------------------------
    @Override
    public List<StepRecord> getStepRecordsClaimedByNode(String nodeRecordId) {
        val query = new Query();
        query.addCriteria(Criteria
                .where("nodeId").is(nodeRecordId)
                .and("status").is(ExecutionStatus.RUNNING)
        );
       return mongoTemplate.find(query, StepRecord.class);
    }

    //--------------------------------------
    @Override
    public List<StepRecord> getFailedStepRecordsForJob(String jobRecordId) {
        val query = new Query();
        query.addCriteria(Criteria
                .where("jobRecordId").is(jobRecordId)
                .and("status").is(ExecutionStatus.FAILED)
        );
        return mongoTemplate.find(query, StepRecord.class);
    }

    //--------------------------------------
    @Override
    public StepRecord tryClaimNextStepRecord(String thisNodeId) {
        val query = new Query();
        query.addCriteria(
                Criteria.where("claimed").is(false)
        );

        val update = new Update();
        update.set("claimed", true);
        update.set("nodeId", thisNodeId);
        val claimedStepRec = mongoTemplate.findAndModify(query, update, StepRecord.class);
        if(claimedStepRec != null) { //because this is the pre update value
            claimedStepRec.setClaimed(true);
            claimedStepRec.setNodeId(thisNodeId);
        }
        return claimedStepRec;
    }

    //--------------------------------------
    @Override
    public void clearAll() {
        mongoTemplate.dropCollection(JobRecord.class);
        mongoTemplate.dropCollection(JobScheduleRecord.class);
        mongoTemplate.dropCollection(BaseNodeRecord.class);
        mongoTemplate.dropCollection(StepRecord.class);
    }
}
