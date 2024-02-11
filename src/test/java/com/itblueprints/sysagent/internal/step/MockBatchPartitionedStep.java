package com.itblueprints.sysagent.internal.step;

import com.itblueprints.sysagent.job.JobArguments;
import com.itblueprints.sysagent.step.Partition;
import com.itblueprints.sysagent.step.PartitionedBatchStep;
import com.itblueprints.sysagent.step.StepContext;
import lombok.val;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MockBatchPartitionedStep implements PartitionedBatchStep<String, String> {

    public boolean onStartCalled;
    public boolean onCompleteCalled;
    public int totalPages;
    public int readPageOfItems_TimesCalled;
    public int processItem_TimesCalled;
    public int writePageOfItems_TimesCalled;
    public List<String> result = new ArrayList<>();

    @Override
    public Page<String> readPageOfItems(Pageable pageRequest, StepContext context) {
        readPageOfItems_TimesCalled++;
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
        writePageOfItems_TimesCalled++;
        result.addAll(page.toList());
        totalPages = page.getTotalPages();
    }

    @Override
    public void onStart(StepContext context) {
        onStartCalled = true;
    }


    @Override
    public void onComplete(StepContext context) {
        onCompleteCalled = true;
    }

    @Override
    public boolean isSelectionFixed() {
        return true;
    }

    @Override
    public List<Partition> getPartitions(JobArguments jobArguments) {
        return List.of(1,2,3).stream().map(i -> Partition.from(Map.of("partition", i))).collect(Collectors.toList());
    }

    @Override
    public String getName(){
        return "Step";
    }
}