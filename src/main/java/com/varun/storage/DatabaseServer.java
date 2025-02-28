package com.varun.storage;

import com.varun.model.DatabaseRequest;
import com.varun.model.RequestFactory;
import com.varun.util.MessageQueue;

public class DatabaseServer implements Runnable {

    private final int processId;

    private final Database database;

    public DatabaseServer(int processId, int totalProcessCount) {
        this.processId = processId;
        this.database = new Database(processId, totalProcessCount);
    }

    /**
     * Reads message from user input and parses it to {@link DatabaseRequest}.
     * Upon parsing invokes processing of the request.
     */
    @SuppressWarnings("InfiniteLoopStatement")
    @Override
    public void run() {
        System.out.printf("Database server started for processId: %d\n", this.processId);
        while (true) {
            try {
                String message = MessageQueue.getInstance().consumeMessage(this.processId);
                System.out.printf("Received message: %s on process: %d \n", message, this.processId);
                DatabaseRequest request = RequestFactory.parseRequest(message);
                request.process(database);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
