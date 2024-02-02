package com.itblueprints.sysagent.step;

import com.itblueprints.sysagent.Arguments;
import lombok.val;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class MockStep implements PartitionedBatchStep<String, String> {

    public boolean preProcessCalled;
    public boolean postProcessCalled;
    public int totalPages;
    public int readChunkOfItems_TimesCalled;
    public int processItem_TimesCalled;
    public List<String> result = new ArrayList<>();

    @Override
    public Page<String> readPageOfItems(Pageable pageRequest, BatchStepContext context) {
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
    public String processItem(String item, BatchStepContext context) {
        processItem_TimesCalled++;
        return item+"_X";
    }

    @Override
    public void writePageOfItems(Page<String> page, BatchStepContext context) {
        result.addAll(page.toList());
        totalPages = page.getTotalPages();
    }

    @Override
    public void onStart(BatchStepContext context) {
        preProcessCalled = true;
    }


    @Override
    public void onComplete(BatchStepContext context) {
        postProcessCalled = true;
    }

    @Override
    public boolean isSelectionFixed() {
        return true;
    }

    @Override
    public List<Arguments> getPartitionArgumentsList(Arguments jobArguments) {
        return List.of(1,2,3).stream().map(i -> {
            val arg = new Arguments();
            arg.put("partition", i);
            return arg;
        }).collect(Collectors.toList());
    }

    @Override
    public String getName(){
        return "Step";
    }
}