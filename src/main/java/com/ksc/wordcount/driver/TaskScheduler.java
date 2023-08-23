package com.ksc.wordcount.driver;

import com.ksc.wordcount.rpc.Driver.DriverRpc;
import com.ksc.wordcount.task.TaskContext;
import com.ksc.wordcount.task.TaskStatus;
import com.ksc.wordcount.task.TaskStatusEnum;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class TaskScheduler {

    private TaskManager taskManager;
    private ExecutorManager executorManager ;

    /**
     * taskId和ExecutorUrl的映射
     */
    private Map<Integer,String> taskExecuotrMap=new HashMap<>();

    public TaskScheduler(TaskManager taskManager, ExecutorManager executorManager) {
        this.taskManager = taskManager;
        this.executorManager = executorManager;
    }

    public void submitTask(int stageId) {
        BlockingQueue<TaskContext> taskQueue = taskManager.getBlockingQueue(stageId);

        while (!taskQueue.isEmpty()) {
            //todo done 学生实现 轮询给各个executor派发任务
            // 1. 获取executor的可用核数
            // 2. 如果可用核数大于0，从taskQueue中取出一个task，派发给executor
            // 3. 如果可用核数等于0，等待1s，继续轮询
            // 4. 如果taskQueue为空，等待1s，继续轮询
            // 5. 如果taskQueue不为空，继续轮询
            // 6. 如果taskQueue为空，且所有executor的可用核数均为0，说明所有task均已派发，跳出循环

            executorManager.getExecutorAvailableCoresMap().forEach((executorUrl,availableCores)->{
                if(availableCores>0 && !taskQueue.isEmpty()){
                    TaskContext task = taskQueue.poll();
                    taskExecuotrMap.put(task.getTaskId(),executorUrl);
                    executorManager.updateExecutorAvailableCores(executorUrl,-1);
                    DriverRpc.submit(executorUrl,task);

                }
            });
            try {
                String executorAvailableCoresMapStr=executorManager.getExecutorAvailableCoresMap().toString();
                System.out.println("TaskScheduler submitTask stageId:"+stageId+",taskQueue size:"+taskQueue.size()+", executorAvailableCoresMap:" + executorAvailableCoresMapStr+ ",sleep 1000");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void waitStageFinish(int stageId){
        StageStatusEnum stageStatusEnum = taskManager.getStageTaskStatus(stageId);
        while (stageStatusEnum==StageStatusEnum.RUNNING){
            try {
                System.out.println("TaskScheduler waitStageFinish stageId:"+stageId+",sleep 1000");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            stageStatusEnum = taskManager.getStageTaskStatus(stageId);
        }
        if(stageStatusEnum == StageStatusEnum.FAILED){
            //如果stage执行失败，需要将该stage下的所有task重新加入到taskQueue中
            //可以从Map<Integer, List<Integer>> stageMap = new HashMap<>();中通过stageId获取到该stage下的所有task
            //然后将这些task重新加入到taskQueue中
            List<Integer> taskList = taskManager.getStageMap().get(stageId);
            for (Integer taskId : taskList) {
                //todo done 学生实现 将该stage下的所有task重新加入到taskQueue中
                //可以利用Map<Integer,BlockingQueue<TaskContext>> stageIdToBlockingQueueMap通过stageId获取到taskQueue
                //然后将task重新加入到taskQueue中
                BlockingQueue<TaskContext> taskQueue = taskManager.getBlockingQueue(stageId);
                TaskContext taskContext = new TaskContext("urlTopN_001",Integer.toString(stageId),taskId,0);
                taskQueue.offer(taskContext);

            }



//            System.err.println("stageId:"+stageId+" failed");
//            System.exit(1);
        }
    }

    public void updateTaskStatus(TaskStatus taskStatus){
        if(taskStatus.getTaskStatus().equals(TaskStatusEnum.FINISHED)||taskStatus.getTaskStatus().equals(TaskStatusEnum.FAILED)){
            String executorUrl=taskExecuotrMap.get(taskStatus.getTaskId());
            executorManager.updateExecutorAvailableCores(executorUrl,1);
        }
    }


}
