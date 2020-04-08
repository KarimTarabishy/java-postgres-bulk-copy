package ai.mendel.core.dbcopy;

import ai.mendel.core.dbcopy.input.InputItem;
import ai.mendel.core.dbcopy.tasks.Copy;
import ai.mendel.core.dbcopy.tasks.Read;
import ai.mendel.core.dbcopy.tasks.Transform;
import ai.mendel.core.utils.MendelUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.ListIterator;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CopyManager {
    private static final Logger logger = Logger.getLogger(CopyManager.class.getName());


    public static void startCopyToTable(BulkCopyBuilder configs) throws ExecutionException, InterruptedException {
        BlockingQueue<InputItem> pendingTransform = new ArrayBlockingQueue<>(100000);
        BlockingQueue<InputItem> pendingWrite = new ArrayBlockingQueue<>(20000);
        AtomicBoolean forceStop = new AtomicBoolean(false);
        MendelUtils.VolatileWrapper<Integer> writtenFiles = new MendelUtils.VolatileWrapper<>(0);

        Future<Long> readTask, copyTask;

        int threads = Runtime.getRuntime().availableProcessors() * (configs.threadsMultiplier) + 2;

        ExecutorService service =  Executors.newFixedThreadPool(threads);
        try {
            Read reader = new Read(configs.inputTSVPath, pendingTransform, forceStop, threads - 2);
            readTask = service.submit(reader);
            copyTask = service.submit(new Copy(pendingWrite, configs.escapedColumns,
                    configs.fieldsMapper, configs.copyAsTSV, configs.nullString, configs.url, configs.user,
                    configs.password, configs.tableName, threads - 2, writtenFiles, forceStop));
            ArrayList<Future<?>> tasks = IntStream.range(0, threads - 2)
                    .mapToObj(i -> service.submit(
                            new Transform(configs.inputTransformers, configs.pointerTSVColIndex,
                                    configs.pointerTSVColTransform, pendingWrite, pendingTransform, forceStop)))
                    .collect(Collectors.toCollection(ArrayList::new));
            tasks.addAll(Arrays.asList(readTask, copyTask));

            logger.info("All threads started, starting monitoring...");
            long lastLog = System.currentTimeMillis();
            while (!tasks.isEmpty()) {
                ListIterator<Future<?>> iterator = tasks.listIterator();
                while (iterator.hasNext()) {
                    Future<?> f = iterator.next();
                    if (f.isDone()) {
                        f.get();
                        iterator.remove();
                    }
                }

                if (System.currentTimeMillis() - lastLog > 1 * 60 * 1000) {
                    logger.info("\nPending to copy: " + pendingWrite.size() +
                            ", copied: " + writtenFiles.getValue() + ", listing file: "
                            + reader.getCurrentIndex() + "/" + reader.getFilesCount());
                    lastLog = System.currentTimeMillis();
                }
                Thread.sleep(5*1000);
            }
            logger.info("Copy done");
            logger.info("Total files read: " + readTask.get() + "\n" +
                    "Total copied files: " + writtenFiles.getValue() + "\n" +
                    "Copied rows: " + copyTask.get());

        }
        finally {
            try {
                shutdown(service, forceStop);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static void shutdown(ExecutorService service, AtomicBoolean forceStop) throws InterruptedException {
        logger.info("Shutting down threads...");
        forceStop.set(true);
        service.shutdownNow();
        logger.info("Waiting 30 seconds for shutdown...");
        if(!service.awaitTermination(30, TimeUnit.SECONDS)){
            logger.info("Couldn't close threads...");
            System.exit(0);
        }else{
            logger.info("Threads exited.");
        }
    }
}
