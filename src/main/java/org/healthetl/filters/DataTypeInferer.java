package org.healthetl.filters;

public class DataTypeInferer extends Filter{
    @Override
    public void run() {
        read();
    }

    public void read() {
        try {
            String json;
            while((json = input.read()) != null){
                System.out.println(json);
            }
        } catch (Exception e){
            System.out.println(e.getMessage());
        }
    }
}
