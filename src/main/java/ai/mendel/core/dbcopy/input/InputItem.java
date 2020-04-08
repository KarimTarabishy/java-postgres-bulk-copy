package ai.mendel.core.dbcopy.input;

import java.util.ArrayList;

public class InputItem {
    ArrayList<String> data = new ArrayList<>();

    public InputItem(String line, boolean isTSV){
        line = line.trim();
        char sep = isTSV ? '\t':',';

        int last_index = 0;
        while(true){
            int sep_index = line.indexOf(sep, last_index);
            String added="";
            if( sep_index == -1){
                added = line.substring(last_index);
            }
            else{
                added = line.substring(last_index, sep_index);
                last_index = sep_index+1;
            }
            if(added.trim().isEmpty()){
                data.add(null);
            }
            else{
                data.add(added);
            }
            if(sep_index == -1){
                break;
            }
        }
    }

    public ArrayList<String> getData(){
        return data;
    }

    public void nullifyData(){
        this.data = null;
    }


}
