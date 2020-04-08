package ai.mendel.core.dbcopy.tasks;

import ai.mendel.core.dbcopy.input.IndexToColumnMapper;
import ai.mendel.core.dbcopy.input.InputItem;
import ai.mendel.core.dbcopy.transformers.InputTransformer;
import ai.mendel.core.utils.GCStorage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Transform implements Callable<Void> {
    private ArrayList<InputTransformer> transformers;
    private  BlockingQueue<InputItem> pendingWrite;
    private BlockingQueue<InputItem> pendingTransform;
    private AtomicBoolean forceStop;
    private Integer pointerTSVColIndex;
    private Function<String, String> pointerTSVColTransformer;
    public Transform(ArrayList<InputTransformer> transformers,
                     Integer pointerTSVColIndex, Function<String, String> pointerTSVColTransformer,
                     BlockingQueue<InputItem> pendingWrite, BlockingQueue<InputItem> pendingTransform,
                     AtomicBoolean forceStop){
        this.transformers = transformers;
        this.pendingTransform = pendingTransform;
        this.pendingWrite = pendingWrite;
        this.forceStop = forceStop;
        this.pointerTSVColIndex = pointerTSVColIndex;
        this.pointerTSVColTransformer = pointerTSVColTransformer;
    }
    @Override
    public Void call() throws Exception {
        GCStorage storage = new GCStorage();
        while (true) {
            if (forceStop.get() || Thread.currentThread().isInterrupted()) {
                return null;
            }
            InputItem item = null;
            try {
                item = pendingTransform.poll(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {  // if thread is requested to stop, , cancel copy and stop it
                System.out.println("Interrupted");
                Thread.currentThread().interrupt();
                break;
            }

            if(item == null){
                continue;
            }

            if(item.getData() == null){
                addToQueue(item);
                break;
            }

            ArrayList<InputItem> finalItems = new ArrayList<>();
            if(pointerTSVColIndex != null && pointerTSVColIndex > -1){
                String pointer = pointerTSVColTransformer.apply(item.getData().get(pointerTSVColIndex));
                String data = storage.object(pointer).getContent(true);
                if(data == null){
                    throw new RuntimeException("File not found " + pointer);
                }
                String [] lines = data.split("\n");
                finalItems.addAll(Arrays.stream(lines)
                        .filter(s -> !s.trim().isEmpty())
                        .map(s -> new InputItem(s, true))
                        .collect(Collectors.toCollection(ArrayList::new)));
            }
            else {
                finalItems.add(item);
            }

            for(InputItem i : finalItems) {
                for (InputTransformer transformer : transformers) {
                    transformer.transform(i);
                }
                if (!addToQueue(i)) {
                    return null;
                }
            }
        }
        return null;
    }

    private boolean addToQueue(InputItem item){
        try {
            while (!forceStop.get() && !Thread.currentThread().isInterrupted()) {
                boolean inserted = pendingWrite.offer(item, 1,
                        TimeUnit.SECONDS);
                if(inserted) {
                    return true;
                }
            }
        }
        catch (InterruptedException ex){
            Thread.currentThread().interrupt();
        }
        return false;
    }
}
