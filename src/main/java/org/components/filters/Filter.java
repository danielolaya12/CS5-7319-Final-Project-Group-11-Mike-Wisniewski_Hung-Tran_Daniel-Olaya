package org.components.filters;


import org.components.connectors.Pipe;

public abstract class Filter implements Runnable{

    protected Pipe input, output;

    public void setIn(Pipe inputPipe) {
        input = inputPipe;
    }

    public void setOut(Pipe outputPipe) {
        output = outputPipe;
    }
}