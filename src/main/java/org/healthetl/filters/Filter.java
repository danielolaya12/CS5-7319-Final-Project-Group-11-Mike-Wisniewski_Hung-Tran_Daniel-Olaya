package org.healthetl.filters;


import org.healthetl.connectors.Pipe;

public abstract class Filter implements Runnable{

    protected Pipe input, output;

    public void setIn(Pipe inputPipe) {
        input = inputPipe;
    }

    public void setOut(Pipe outputPipe) {
        output = outputPipe;
    }
}