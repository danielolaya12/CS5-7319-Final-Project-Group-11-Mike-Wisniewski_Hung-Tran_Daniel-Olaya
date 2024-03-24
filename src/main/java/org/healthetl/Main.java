package org.healthetl;

import org.healthetl.connectors.Pipe;
import org.healthetl.filters.ApiReader;
import org.healthetl.filters.Filter;

public class Main {
    public static void main(String[] args) {
        Filter[] filters = new Filter[] {new ApiReader(), new ApiReader()};
        setOut(filters);
        startFilters(filters);
    }

    //Set single output
    public static void setOut(Filter[] filters){
        Pipe p = new Pipe();
        for(Filter filer: filters){
            filer.setOut(p);
        }
    }
    private static void startFilters(Runnable[] filters) {
        for (Runnable filter : filters) {
            Thread thread = new Thread(filter);
            thread.start();
        }
    }

    public static void connectFilters(Filter[] filters) {
        for (int i = 0; i < filters.length - 1; i++) {
            Pipe p = new Pipe();
            filters[i].setOut(p);
            filters[i + 1].setIn(p);
        }
    }
}