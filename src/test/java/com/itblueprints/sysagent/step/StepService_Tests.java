package com.itblueprints.sysagent.step;

import com.itblueprints.sysagent.Arguments;
import com.itblueprints.sysagent.Config;
import com.itblueprints.sysagent.ThreadManager;
import com.itblueprints.sysagent.cluster.NodeInfo;
import com.itblueprints.sysagent.job.JobService;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StepService_Tests {

    @Mock MongoTemplate mongoTemplate;
    @Mock JobService jobService;
    @Mock Config config;
    @Mock ThreadManager threadManager;

    StepService stepService;

    private final String jobName = "Job";
    private final String stepName = "Step";
    private ExecutorService executor = Executors.newSingleThreadExecutor();

    //-------------------------------------
    @BeforeEach
    void beforeEach() {
        when(threadManager.getBatchPageSize()).thenReturn(4);
        when(threadManager.getExecutor()).thenReturn(executor);
        when(threadManager.getWorkerTaskQueuSize()).thenReturn(4);

        stepService = new StepService(mongoTemplate, jobService, threadManager);
    }

    //-------------------------------------
    @Test
    void onHeartBeat() {
        val nodeInfo = new NodeInfo();

        val stepRec = createStepRecord();
        when(mongoTemplate.findAndModify(any(), any(), any())).thenReturn(stepRec);

        val step = new MockStep();
        when(jobService.getStep(jobName, stepName)).thenReturn(step);

        val now = LocalDateTime.of(2024, 1, 10, 0,0,0);
        stepService.onHeartBeat(nodeInfo, now);

        assertEquals(StepRecord.Status.Completed, stepRec.getStatus());
        assertEquals(3, step.totalPages);
        assertEquals(true, step.preProcessCalled);
        assertEquals(true, step.postProcessCalled);
        assertEquals(3, step.readChunkOfItems_TimesCalled);
        assertEquals(11, step.processItem_TimesCalled);
        assertEquals(List.of("A_X", "B_X", "C_X", "D_X", "E_X", "F_X", "G_X", "H_X", "I_X", "J_X", "K_X"), step.result);
    }

    //------------------------------------
    private StepRecord createStepRecord(){
        val stepRec = new StepRecord();
        stepRec.setJobName(jobName);
        stepRec.setStepName(stepName);
        stepRec.setClaimed(true);
        val jobArgs = new Arguments();
        jobArgs.put("procDate", LocalDate.of(2024, 1, 12));
        jobArgs.put("pmtProfile", "state_pension");
        stepRec.setJobArguments(jobArgs);
        val prtArgs = new Arguments();
        prtArgs.put("custProfile", "resident_GB");
        prtArgs.put("partition", 1);
        stepRec.setPartitionArguments(prtArgs);
        stepRec.setPartitionNum(0);
        stepRec.setPartitionCount(10);
        return stepRec;
    }

    //--------------------------------
    static class MockStep implements BatchStep<String, String> {

        public boolean preProcessCalled;
        public boolean postProcessCalled;
        public int totalPages;
        public int readChunkOfItems_TimesCalled;
        public int processItem_TimesCalled;
        public List<String> result = new ArrayList<>();

        @Override
        public void preProcess(StepContext context) {
            preProcessCalled = true;
        }

        @Override
        public Page<String> readPageOfItems(Pageable pageRequest, StepContext context) {
            readChunkOfItems_TimesCalled++;
            if(pageRequest.getPageNumber() == 0) {
                val items = List.of("A", "B", "C", "D");
                val pg = new PageImpl<>(items, pageRequest, 11);
                return pg;
            }
            else if(pageRequest.getPageNumber() == 1){
                val items = List.of("E", "F", "G", "H");
                val pg = new PageImpl<>(items, pageRequest, 11);
                return pg;
            }
            else {
                val items = List.of("I", "J", "K");
                val pg = new PageImpl<>(items, pageRequest, 11);
                return pg;
            }
        }

        @Override
        public String processItem(String item, StepContext context) {
            processItem_TimesCalled++;
            return item+"_X";
        }

        @Override
        public void writePageOfItems(Page<String> page, StepContext context) {
            result.addAll(page.toList());
            totalPages = page.getTotalPages();
        }

        @Override
        public void postProcess(StepContext context) {
            postProcessCalled = true;
        }

        @Override
        public boolean isSelectionFixed() {
            return true;
        }

        @Override
        public List<Arguments> getPartitionArguments(Arguments jobArguments) {
            return List.of();
        }

        @Override
        public String getName(){
            return "Step";
        }
    }

}