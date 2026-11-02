package com.rcbill.notification.application;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
public final class BoundedExecution implements AutoCloseable {
    private final int maximum;private final Semaphore capacity;
    private final ExecutorService executor=Executors.newVirtualThreadPerTaskExecutor();
    private final AtomicBoolean closed=new AtomicBoolean();
    public BoundedExecution(int maximum) {if(maximum<1)throw new IllegalArgumentException("maximum");this.maximum=maximum;capacity=new Semaphore(maximum);}
    public Optional<Reservation> reserve(){
        if(closed.get()||!capacity.tryAcquire())return Optional.empty();
        if(closed.get()){capacity.release();return Optional.empty();}
        return Optional.of(new Reservation(){
            final AtomicInteger state=new AtomicInteger();
            void release(){if(state.getAndSet(2)!=2)capacity.release();}
            public void execute(Runnable task){
                if(!state.compareAndSet(0,1))throw new IllegalStateException("Reservation already consumed");
                try{executor.execute(()->{try{task.run();}finally{release();}});}
                catch(RuntimeException rejected){release();throw rejected;}
            }
            public void close(){if(state.compareAndSet(0,2))capacity.release();}
        });
    }
    public int inFlight(){return maximum-capacity.availablePermits();}
    public void close(){closed.set(true);executor.close();}
    public interface Reservation extends AutoCloseable {void execute(Runnable task);void close();}
}
