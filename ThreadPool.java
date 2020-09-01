package il.co.ilrd.threadpool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import il.co.ilrd.waitable_queue.WaitableQueueSem;

public class ThreadPool implements Executor {	
	private List<TPThread> threadsList = new ArrayList<>();
	private int numOfThreads;
	private boolean isShutdown = false;
	private Semaphore pauseSem = new Semaphore(0);
	private Semaphore awaitSem = new Semaphore(0);
	private WaitableQueueSem<ThreadPoolTask<?>> tasksQueue = 
													new WaitableQueueSem<>();
	private final static int MAX_VIP_PRIORITY = TaskPriority.NUM_PRIORITY.ordinal();
	private final static int MIN_VIP_PRIORITY = TaskPriority.MIN.ordinal() - 1;
	private final static int DEAFULT_NUM_THREADS = 
									Runtime.getRuntime().availableProcessors();

	public enum TaskPriority {
		MIN,
		NORM,
		MAX,
		NUM_PRIORITY
	}
	
	public ThreadPool() {
		this(DEAFULT_NUM_THREADS);
	}
	
	public <T> ThreadPool(int numOfThreads) {
		if(0 > numOfThreads) {
			throw new InvalidParameter();
		}
		
		this.numOfThreads = numOfThreads;
		createAndStartThread(numOfThreads);
	}
	
	private void createAndStartThread(int numOfThreads) {
		for (int i = 0; i < numOfThreads; i++) {
			threadsList.add(new TPThread());
		}
		
		for (TPThread tpThread : threadsList) {
			tpThread.start();
		}
	}
	
	/*---------------------------- Class TPThread ----------------------------*/
	
	private class TPThread extends Thread {
		private ThreadPoolTask<?> curTask = null;
		private boolean toRun = true;
		
		@Override
		public void run() {
			while(toRun) {
				try {
					curTask = tasksQueue.dequeue();
					if(!curTask.getFuture().isCancelled()) {
						curTask.runTask();
					}
				} catch (Exception e) {
					curTask.getFuture().taskException = e;
					curTask.runTaskSem.release();
				}
				
			}
			
			threadsList.remove(this);
			
			if(threadsList.isEmpty()) {
				awaitSem.release();
			}
		}
	}
	
	/*---------------------------- API functions -----------------------------*/

	private <T> Future<T> submitTask(Callable<T> callable, int taskPriority) {
		if(true == isShutdown) {
			throw new SubmitAfterShutdownException();
		}
		
		ThreadPoolTask<T> task = new ThreadPoolTask<>(callable, taskPriority);
		tasksQueue.enqueue(task);
		
		return task.getFuture();
	}
	
	public <T> Future<T> submitTask(Callable<T> callable) {
		return submitTask(callable, TaskPriority.NORM.ordinal());
	}
	
	public <T> Future<T> submitTask(Callable<T> callable, 
											TaskPriority taskPriority) {
		return submitTask(callable, taskPriority.ordinal());
	}
	
	public Future<Void> submitTask(Runnable runnable, 
											TaskPriority taskPriority) {
		return submitTask(Executors.callable(runnable, null), 
													taskPriority.ordinal());
	}
	
	public <T> Future<T> submitTask(Runnable runnable, 
										TaskPriority taskPriority, T t) {
		return submitTask(Executors.callable(runnable,t), 
												taskPriority.ordinal());
	}
	
	public void setNumberOfThread(int numOfThreads) {
		if(this.numOfThreads < numOfThreads) {
			createAndStartThread(numOfThreads - this.numOfThreads);
		}
		else {
			int numOfTasksToAdd = this.numOfThreads - numOfThreads;
			addStopTasks(numOfTasksToAdd, MAX_VIP_PRIORITY);
		}
		
		this.numOfThreads = numOfThreads;
	}
	
	@Override
	public void execute(Runnable runnable) {
		submitTask(runnable, TaskPriority.NORM);
	}
	
	public void pause() {
		for(int numOfSubmit = 0; numOfSubmit < numOfThreads; ++numOfSubmit) {
			submitTask(new PauseTask<>(), MAX_VIP_PRIORITY);
		}
	}
	
	public void resume() {
		pauseSem.release(numOfThreads);
	}
	
	public void shutdown() {
		addStopTasks(numOfThreads, MIN_VIP_PRIORITY);
		
		numOfThreads = 0;
		isShutdown = true;
	}

	public boolean awaitTermination(long timeout, TimeUnit unit) 
												throws InterruptedException {
		return awaitSem.tryAcquire(timeout, unit);
	}
	
	private <T> void addStopTasks(int numOfTasks, int priority) {
		for(int numOfSubmit = 0; numOfSubmit < numOfTasks; ++numOfSubmit) {
			submitTask(new StopTask<>(), priority);
		}
	}
	
	/*------------------------- Class ThreadPoolTask -------------------------*/

	private class ThreadPoolTask<T> implements Comparable<ThreadPoolTask<T>> {	
		private int taskPriority;
		private Callable<T> callable;
		private TaskFuture taskFuture = new TaskFuture();
		private Semaphore runTaskSem = new Semaphore(0);
		
		public ThreadPoolTask(Callable<T> callable, int taskPriority) {
			this.callable = callable;
			this.taskPriority = taskPriority;
		}
		
		public TaskFuture getFuture() {
			return taskFuture;
		}

		@Override
		public int compareTo(ThreadPoolTask<T> otherTask) {
			return otherTask.taskPriority - taskPriority;
		}
		
		private void runTask() throws Exception {
			taskFuture.returnObj = callable.call();
			taskFuture.isDone = true;
			runTaskSem.release();
		}
		
		/*------------------------- Class TaskFuture -------------------------*/
		
		private class TaskFuture implements Future<T> {
			private boolean isDone = false;
			private boolean isCancelled = false;
			private T returnObj = null;
			private Throwable taskException = null;
			
			@Override
			public boolean cancel(boolean arg0) {
				if(tasksQueue.remove(ThreadPoolTask.this)) {
					isCancelled = true;
					isDone = true;
				}

				return isCancelled;
			}

			@Override
			public T get() throws InterruptedException, ExecutionException {
				runTaskSem.acquire();
				
				checkException();

				return returnObj;
			}

			@Override
			public T get(long timeout, TimeUnit unit) 
						throws InterruptedException, ExecutionException, 
													TimeoutException {
				runTaskSem.tryAcquire(timeout, unit);
				
				checkException();
				
				return returnObj;
			}

			@Override
			public boolean isCancelled() {
				return isCancelled;
			}

			@Override
			public boolean isDone() {
				return isDone;
			}
			
			private void checkException() throws ExecutionException {
				if(null != taskException) {
					throw new ExecutionException(taskException);
				}
			}
			
		}
	}
	
	/*------------------------- StopTask & PauseTask -------------------------*/
	
	private class StopTask<T> implements Callable<T> {
		@Override
		public T call() throws Exception {
			TPThread currThread = (TPThread) Thread.currentThread();
			currThread.toRun = false;
			return null;
		}
		
	}
	
	private class PauseTask<T> implements Callable<T> {

		@Override
		public T call() throws Exception {
			pauseSem.acquire();
	
			return null;
		}

	}
	
	/*------------------------- Class Exception -------------------------*/

	private class SubmitAfterShutdownException extends RuntimeException {
		private static final long serialVersionUID = 1L;
		
	}
	
	private class InvalidParameter extends RuntimeException {
		private static final long serialVersionUID = 1L;
		
	}
}
