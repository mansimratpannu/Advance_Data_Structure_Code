import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.stream.*;
import java.io.*;
import java.net.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.security.MessageDigest;
import java.nio.charset.StandardCharsets;

// ==================== Task Definition ====================
enum TaskPriority {
    LOW(1), MEDIUM(5), HIGH(10), CRITICAL(20);
    
    private final int weight;
    TaskPriority(int weight) { this.weight = weight; }
    public int getWeight() { return weight; }
}

enum TaskStatus {
    PENDING, QUEUED, RUNNING, COMPLETED, FAILED, CANCELLED, RETRYING
}

class Task implements Serializable, Comparable<Task> {
    private static final AtomicLong idGenerator = new AtomicLong(0);
    
    private final long id;
    private final String name;
    private final TaskPriority priority;
    private final Map<String, Object> payload;
    private volatile TaskStatus status;
    private final Instant createdAt;
    private Instant startedAt;
    private Instant completedAt;
    private int retryCount;
    private final int maxRetries;
    private String assignedNode;
    private Exception lastError;
    private long executionTimeMs;
    
    public Task(String name, TaskPriority priority, Map<String, Object> payload, int maxRetries) {
        this.id = idGenerator.incrementAndGet();
        this.name = name;
        this.priority = priority;
        this.payload = new ConcurrentHashMap<>(payload);
        this.status = TaskStatus.PENDING;
        this.createdAt = Instant.now();
        this.retryCount = 0;
        this.maxRetries = maxRetries;
    }
    
    @Override
    public int compareTo(Task other) {
        int priorityCompare = Integer.compare(other.priority.getWeight(), this.priority.getWeight());
        return priorityCompare != 0 ? priorityCompare : Long.compare(this.id, other.id);
    }
    
    public String getChecksum() throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        String data = id + name + priority + payload.toString();
        byte[] hash = digest.digest(data.getBytes(StandardCharsets.UTF_8));
        return Base64.getEncoder().encodeToString(hash);
    }
    
    // Getters and setters
    public long getId() { return id; }
    public String getName() { return name; }
    public TaskPriority getPriority() { return priority; }
    public Map<String, Object> getPayload() { return payload; }
    public TaskStatus getStatus() { return status; }
    public void setStatus(TaskStatus status) { this.status = status; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getStartedAt() { return startedAt; }
    public void setStartedAt(Instant startedAt) { this.startedAt = startedAt; }
    public Instant getCompletedAt() { return completedAt; }
    public void setCompletedAt(Instant completedAt) { this.completedAt = completedAt; }
    public int getRetryCount() { return retryCount; }
    public void incrementRetryCount() { this.retryCount++; }
    public int getMaxRetries() { return maxRetries; }
    public String getAssignedNode() { return assignedNode; }
    public void setAssignedNode(String assignedNode) { this.assignedNode = assignedNode; }
    public Exception getLastError() { return lastError; }
    public void setLastError(Exception lastError) { this.lastError = lastError; }
    public long getExecutionTimeMs() { return executionTimeMs; }
    public void setExecutionTimeMs(long executionTimeMs) { this.executionTimeMs = executionTimeMs; }
    
    @Override
    public String toString() {
        return String.format("Task[id=%d, name=%s, priority=%s, status=%s, node=%s]",
            id, name, priority, status, assignedNode);
    }
}

// ==================== Task Executor ====================
interface TaskExecutor {
    void execute(Task task) throws Exception;
}

class DefaultTaskExecutor implements TaskExecutor {
    @Override
    public void execute(Task task) throws Exception {
        Map<String, Object> payload = task.getPayload();
        String operation = (String) payload.getOrDefault("operation", "compute");
        
        switch (operation) {
            case "compute":
                performComputation(task);
                break;
            case "io":
                performIOOperation(task);
                break;
            case "network":
                performNetworkOperation(task);
                break;
            default:
                performDefaultOperation(task);
        }
    }
    
    private void performComputation(Task task) throws InterruptedException {
        int complexity = (int) task.getPayload().getOrDefault("complexity", 100);
        long result = 0;
        
        for (int i = 0; i < complexity * 10000; i++) {
            result += Math.sqrt(i) * Math.log(i + 1);
            if (i % 50000 == 0) {
                Thread.sleep(10);
            }
        }
        
        task.getPayload().put("result", result);
    }
    
    private void performIOOperation(Task task) throws Exception {
        String filename = "temp_" + task.getId() + ".txt";
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
            for (int i = 0; i < 1000; i++) {
                writer.write("Task " + task.getId() + " - Line " + i + "\n");
            }
        }
        Thread.sleep(500);
        new File(filename).delete();
    }
    
    private void performNetworkOperation(Task task) throws Exception {
        String url = (String) task.getPayload().getOrDefault("url", "https://www.example.com");
        try {
            URL urlObj = new URL(url);
            HttpURLConnection conn = (HttpURLConnection) urlObj.openConnection();
            conn.setRequestMethod("HEAD");
            conn.setConnectTimeout(2000);
            conn.setReadTimeout(2000);
            int responseCode = conn.getResponseCode();
            task.getPayload().put("response_code", responseCode);
            conn.disconnect();
        } catch (Exception e) {
            task.getPayload().put("error", e.getMessage());
        }
    }
    
    private void performDefaultOperation(Task task) throws InterruptedException {
        Thread.sleep((long) task.getPayload().getOrDefault("duration", 1000));
    }
}

// ==================== Worker Node ====================
class WorkerNode implements Runnable {
    private final String nodeId;
    private final BlockingQueue<Task> taskQueue;
    private final TaskExecutor executor;
    private final AtomicBoolean running;
    private final AtomicInteger tasksProcessed;
    private final MetricsCollector metrics;
    private volatile Task currentTask;
    
    public WorkerNode(String nodeId, BlockingQueue<Task> taskQueue, 
                     TaskExecutor executor, MetricsCollector metrics) {
        this.nodeId = nodeId;
        this.taskQueue = taskQueue;
        this.executor = executor;
        this.running = new AtomicBoolean(true);
        this.tasksProcessed = new AtomicInteger(0);
        this.metrics = metrics;
    }
    
    @Override
    public void run() {
        System.out.println("[" + nodeId + "] Worker started");
        
        while (running.get()) {
            try {
                currentTask = taskQueue.poll(100, TimeUnit.MILLISECONDS);
                
                if (currentTask != null) {
                    processTask(currentTask);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        
        System.out.println("[" + nodeId + "] Worker stopped. Tasks processed: " + tasksProcessed.get());
    }
    
    private void processTask(Task task) {
        task.setStatus(TaskStatus.RUNNING);
        task.setStartedAt(Instant.now());
        task.setAssignedNode(nodeId);
        
        long startTime = System.currentTimeMillis();
        
        try {
            System.out.printf("[%s] Executing %s (Priority: %s)%n", 
                nodeId, task.getName(), task.getPriority());
            
            executor.execute(task);
            
            long executionTime = System.currentTimeMillis() - startTime;
            task.setExecutionTimeMs(executionTime);
            task.setCompletedAt(Instant.now());
            task.setStatus(TaskStatus.COMPLETED);
            
            tasksProcessed.incrementAndGet();
            metrics.recordTaskCompletion(task, true);
            
            System.out.printf("[%s] Completed %s in %dms%n", 
                nodeId, task.getName(), executionTime);
            
        } catch (Exception e) {
            task.setLastError(e);
            task.setStatus(TaskStatus.FAILED);
            metrics.recordTaskCompletion(task, false);
            
            System.err.printf("[%s] Failed %s: %s%n", 
                nodeId, task.getName(), e.getMessage());
        }
    }
    
    public void shutdown() {
        running.set(false);
    }
    
    public String getNodeId() { return nodeId; }
    public int getTasksProcessed() { return tasksProcessed.get(); }
    public Task getCurrentTask() { return currentTask; }
    public boolean isIdle() { return currentTask == null; }
}

// ==================== Metrics Collector ====================
class MetricsCollector {
    private final AtomicLong totalTasksSubmitted = new AtomicLong(0);
    private final AtomicLong totalTasksCompleted = new AtomicLong(0);
    private final AtomicLong totalTasksFailed = new AtomicLong(0);
    private final ConcurrentHashMap<TaskPriority, AtomicLong> tasksByPriority = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<Long> executionTimes = new ConcurrentLinkedQueue<>();
    private final int maxExecutionTimeSamples = 1000;
    
    public MetricsCollector() {
        for (TaskPriority priority : TaskPriority.values()) {
            tasksByPriority.put(priority, new AtomicLong(0));
        }
    }
    
    public void recordTaskSubmission(Task task) {
        totalTasksSubmitted.incrementAndGet();
        tasksByPriority.get(task.getPriority()).incrementAndGet();
    }
    
    public void recordTaskCompletion(Task task, boolean success) {
        if (success) {
            totalTasksCompleted.incrementAndGet();
            executionTimes.offer(task.getExecutionTimeMs());
            
            if (executionTimes.size() > maxExecutionTimeSamples) {
                executionTimes.poll();
            }
        } else {
            totalTasksFailed.incrementAndGet();
        }
    }
    
    public Map<String, Object> getMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("total_submitted", totalTasksSubmitted.get());
        metrics.put("total_completed", totalTasksCompleted.get());
        metrics.put("total_failed", totalTasksFailed.get());
        metrics.put("success_rate", calculateSuccessRate());
        metrics.put("avg_execution_time_ms", calculateAverageExecutionTime());
        metrics.put("tasks_by_priority", getTasksByPriority());
        return metrics;
    }
    
    private double calculateSuccessRate() {
        long completed = totalTasksCompleted.get();
        long failed = totalTasksFailed.get();
        long total = completed + failed;
        return total > 0 ? (completed * 100.0) / total : 0.0;
    }
    
    private double calculateAverageExecutionTime() {
        if (executionTimes.isEmpty()) return 0.0;
        return executionTimes.stream()
            .mapToLong(Long::longValue)
            .average()
            .orElse(0.0);
    }
    
    private Map<String, Long> getTasksByPriority() {
        return tasksByPriority.entrySet().stream()
            .collect(Collectors.toMap(
                e -> e.getKey().name(),
                e -> e.getValue().get()
            ));
    }
    
    public void printMetrics() {
        System.out.println("\n========== SYSTEM METRICS ==========");
        Map<String, Object> metrics = getMetrics();
        metrics.forEach((key, value) -> {
            if (value instanceof Map) {
                System.out.println(key + ":");
                ((Map<?, ?>) value).forEach((k, v) -> 
                    System.out.println("  " + k + ": " + v));
            } else {
                System.out.println(key + ": " + value);
            }
        });
        System.out.println("====================================\n");
    }
}

// ==================== Load Balancer ====================
class LoadBalancer {
    private final List<WorkerNode> workers;
    private final AtomicInteger roundRobinCounter = new AtomicInteger(0);
    
    public LoadBalancer(List<WorkerNode> workers) {
        this.workers = new ArrayList<>(workers);
    }
    
    public WorkerNode selectWorker(Task task) {
        return selectLeastLoaded();
    }
    
    private WorkerNode selectRoundRobin() {
        int index = Math.abs(roundRobinCounter.getAndIncrement() % workers.size());
        return workers.get(index);
    }
    
    private WorkerNode selectLeastLoaded() {
        return workers.stream()
            .min(Comparator.comparingInt(WorkerNode::getTasksProcessed))
            .orElse(workers.get(0));
    }
    
    public List<WorkerNode> getWorkers() {
        return new ArrayList<>(workers);
    }
}

// ==================== Task Scheduler ====================
class TaskScheduler {
    private final PriorityBlockingQueue<Task> taskQueue;
    private final List<WorkerNode> workers;
    private final ExecutorService workerPool;
    private final MetricsCollector metrics;
    private final LoadBalancer loadBalancer;
    private final ScheduledExecutorService monitorService;
    private final ConcurrentHashMap<Long, Task> taskRegistry;
    private volatile boolean running;
    
    public TaskScheduler(int numWorkers) {
        this.taskQueue = new PriorityBlockingQueue<>(100);
        this.workers = new CopyOnWriteArrayList<>();
        this.workerPool = Executors.newFixedThreadPool(numWorkers);
        this.metrics = new MetricsCollector();
        this.taskRegistry = new ConcurrentHashMap<>();
        this.running = true;
        
        // Create workers
        TaskExecutor executor = new DefaultTaskExecutor();
        for (int i = 0; i < numWorkers; i++) {
            WorkerNode worker = new WorkerNode(
                "Worker-" + i, 
                taskQueue, 
                executor, 
                metrics
            );
            workers.add(worker);
            workerPool.submit(worker);
        }
        
        this.loadBalancer = new LoadBalancer(workers);
        
        // Start monitoring
        this.monitorService = Executors.newScheduledThreadPool(1);
        monitorService.scheduleAtFixedRate(
            this::printStatus, 
            5, 5, TimeUnit.SECONDS
        );
    }
    
    public CompletableFuture<Task> submitTask(Task task) {
        return CompletableFuture.supplyAsync(() -> {
            task.setStatus(TaskStatus.QUEUED);
            taskRegistry.put(task.getId(), task);
            metrics.recordTaskSubmission(task);
            
            try {
                taskQueue.put(task);
                System.out.println("Submitted: " + task);
            } catch (InterruptedException e) {
                task.setStatus(TaskStatus.FAILED);
                task.setLastError(e);
                Thread.currentThread().interrupt();
            }
            
            return task;
        });
    }
    
    public List<Task> submitBatch(List<Task> tasks) {
        return tasks.stream()
            .map(this::submitTask)
            .map(CompletableFuture::join)
            .collect(Collectors.toList());
    }
    
    public Optional<Task> getTask(long taskId) {
        return Optional.ofNullable(taskRegistry.get(taskId));
    }
    
    public List<Task> getTasksByStatus(TaskStatus status) {
        return taskRegistry.values().stream()
            .filter(task -> task.getStatus() == status)
            .collect(Collectors.toList());
    }
    
    private void printStatus() {
        System.out.println("\n========== SCHEDULER STATUS ==========");
        System.out.println("Queue size: " + taskQueue.size());
        System.out.println("Total tasks tracked: " + taskRegistry.size());
        
        System.out.println("\nWorker Status:");
        workers.forEach(worker -> {
            String status = worker.isIdle() ? "IDLE" : "BUSY: " + worker.getCurrentTask();
            System.out.printf("  %s: %s (Completed: %d)%n", 
                worker.getNodeId(), status, worker.getTasksProcessed());
        });
        
        System.out.println("\nTasks by Status:");
        Arrays.stream(TaskStatus.values()).forEach(status -> {
            long count = taskRegistry.values().stream()
                .filter(t -> t.getStatus() == status)
                .count();
            if (count > 0) {
                System.out.println("  " + status + ": " + count);
            }
        });
        
        System.out.println("======================================\n");
    }
    
    public void shutdown() {
        System.out.println("\nShutting down scheduler...");
        running = false;
        
        monitorService.shutdown();
        workers.forEach(WorkerNode::shutdown);
        workerPool.shutdown();
        
        try {
            if (!workerPool.awaitTermination(10, TimeUnit.SECONDS)) {
                workerPool.shutdownNow();
            }
            monitorService.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            workerPool.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        System.out.println("Scheduler shutdown complete.");
        metrics.printMetrics();
    }
    
    public MetricsCollector getMetrics() {
        return metrics;
    }
}

// ==================== Main Application ====================
public class DistributedTaskSystem {
    public static void main(String[] args) throws Exception {
        System.out.println("=== Distributed Task Processing System ===\n");
        
        TaskScheduler scheduler = new TaskScheduler(4);
        
        // Submit various tasks
        System.out.println("Submitting tasks...\n");
        
        List<Task> tasks = new ArrayList<>();
        
        // Critical tasks
        tasks.add(new Task("Database Backup", TaskPriority.CRITICAL,
            Map.of("operation", "io"), 3));
        tasks.add(new Task("Security Scan", TaskPriority.CRITICAL,
            Map.of("operation", "compute", "complexity", 200), 2));
        
        // High priority tasks
        tasks.add(new Task("Payment Processing", TaskPriority.HIGH,
            Map.of("operation", "network", "url", "https://api.stripe.com"), 3));
        tasks.add(new Task("Data Analysis", TaskPriority.HIGH,
            Map.of("operation", "compute", "complexity", 150), 2));
        
        // Medium priority tasks
        for (int i = 0; i < 5; i++) {
            tasks.add(new Task("Report Generation " + i, TaskPriority.MEDIUM,
                Map.of("operation", "compute", "complexity", 100), 2));
        }
        
        // Low priority tasks
        for (int i = 0; i < 3; i++) {
            tasks.add(new Task("Cache Cleanup " + i, TaskPriority.LOW,
                Map.of("operation", "io"), 1));
        }
        
        // Submit batch
        scheduler.submitBatch(tasks);
        
        // Submit additional tasks during execution
        Thread.sleep(3000);
        scheduler.submitTask(new Task("Emergency Update", TaskPriority.CRITICAL,
            Map.of("operation", "compute", "complexity", 50), 3));
        
        // Wait for tasks to complete
        Thread.sleep(15000);
        
        // Final status
        System.out.println("\n=== Final Task Status ===");
        Arrays.stream(TaskStatus.values()).forEach(status -> {
            List<Task> statusTasks = scheduler.getTasksByStatus(status);
            if (!statusTasks.isEmpty()) {
                System.out.println(status + " (" + statusTasks.size() + "):");
                statusTasks.forEach(t -> System.out.println("  - " + t));
            }
        });
        
        scheduler.shutdown();
        
        System.out.println("\nSystem terminated successfully.");
    }
}