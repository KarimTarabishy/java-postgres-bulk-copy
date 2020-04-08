package ai.mendel.core.dbcopy.tasks;

import ai.mendel.core.dbcopy.input.InputItem;
import ai.mendel.core.utils.GCSURLParser;
import ai.mendel.core.utils.GCStorage;
import ai.mendel.core.utils.MendelUtils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class Read implements Callable<Long> {
    private final String inputGCSPath;
    private AtomicBoolean forceStop;
    private BlockingQueue<InputItem> pendingTransform;
    private ArrayList<String> paths;
    private volatile int currentIndex = 0;

    public Read(String input_gcs_path, BlockingQueue<InputItem> pendingTransform,
                AtomicBoolean forceStop){
        this.inputGCSPath = input_gcs_path;
        this.forceStop = forceStop;
        this.pendingTransform = pendingTransform;
        paths = fetchPathsOrSelf(inputGCSPath);
    }

    public int getFilesCount(){return paths.size();}
    public int getCurrentIndex() {return currentIndex;}

    @Override
    public Long call() throws Exception {
        GCStorage storage = new GCStorage();
        long total = 0L;
        while (currentIndex < paths.size()){
            if(forceStop.get() || Thread.currentThread().isInterrupted()){
                return 0L;
            }
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(storage.object(paths.get(currentIndex)).getInputStream()))) {
                String line;
                while((line = reader.readLine()) != null){
                    if(line.trim().isEmpty()){
                        continue;
                    }
                    InputItem item = new InputItem(line, true);
                    if(addToQueue(item)){
                        total++;
                    }
                    else {
                        return 0L;
                    }
                }
            }
            currentIndex++;
        }
        InputItem end = new InputItem("", true);
        end.nullifyData();

        return addToQueue(end) ? total:0L;
    }

    private boolean addToQueue(InputItem item){
        try {
            while (!forceStop.get() && !Thread.currentThread().isInterrupted()) {
                boolean inserted = pendingTransform.offer(item, 1,
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

    private static ArrayList<String> fetchPathsOrSelf(String path){
        MendelUtils.Pair<String, String> urlSplitted = GCSURLParser.parse(path);
        GCStorage storage = new GCStorage();
        String pathWithSlash = path.endsWith("/") ? path:path+"/";
        String pathWithoutSlash = path.endsWith("/") ? path.substring(0, path.length()-1):path;

        ArrayList<String> pathsWithSlash = storage.object(pathWithSlash).getPaths(true);
        ArrayList<String> paths = pathsWithSlash == null ?
                storage.object(pathWithoutSlash).getPaths(true):pathsWithSlash;
        return paths.stream().map(s -> GCStorage.getURI(urlSplitted.getFirst(), s))
                .collect(Collectors.toCollection(ArrayList::new));

    }
}
