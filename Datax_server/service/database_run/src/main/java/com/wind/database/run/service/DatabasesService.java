package com.wind.database.run.service;

import com.wind.base.exception.NoloseException;

import java.util.concurrent.BlockingQueue;

public interface DatabasesService {
    void runJob(BlockingQueue job, String dataxId,String desc) throws NoloseException, InterruptedException;
}
