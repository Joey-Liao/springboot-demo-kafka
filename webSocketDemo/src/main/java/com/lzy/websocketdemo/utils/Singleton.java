package com.lzy.websocketdemo.utils;

public class Singleton {
    private volatile Singleton sig;
    public Singleton getInstance(){
        if(sig==null){
            synchronized (Singleton.class){
                if(sig==null) sig=new Singleton();
            }
        }
        return sig;
    }
}
