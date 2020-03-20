package com.github.victormpcmun.delayedbatchexecutor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class BatchCallBackExecutionResult {
    private final RuntimeException runtimeException;
    private final List<Object> resizedList;

    BatchCallBackExecutionResult(List<Object> result, RuntimeException runtimeException, int desiredSize) {
        this.runtimeException = runtimeException;
        this.resizedList= resizeListFillingWithNullsIfNecessary(result, desiredSize);
    }

    RuntimeException getThrownRuntimeExceptionOrNull() {
        return runtimeException;
    }

    Object getReturnedResultOrNull(int position) {
        return resizedList.get(position);
    }

    private List<Object> resizeListFillingWithNullsIfNecessary(List<Object> list, int desiredSize) {
        if (list==null) {
            list= Collections.nCopies(desiredSize,  null);
        } else if (list.size()<desiredSize) {
            list = new ArrayList(list); // make it mutable in case it isn't
            list.addAll(Collections.nCopies(desiredSize-list.size(),null));
        }
        return list;
    }
}