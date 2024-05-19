class SynchronizedClosure : public Closure {
public:
    // 使用指定次数初始化
    explicit SynchronizedClosure(int num_signal) : _count(num_signal) {}

    // 在 Run 被调用足够次数后，唤醒所有等待的线程
    void Run() override {
        std::lock_guard<std::mutex> lock(_mutex);
        if (--_count <= 0) {  // 每次调用 Run 减少计数，并在计数为0时通知所有等待线程
            _cv.notify_all();
        }
    }

    // 阻塞当前线程直到 Run 被调用指定的次数
    void wait() {
        std::unique_lock<std::mutex> lock(_mutex);
        _cv.wait(lock, [this] { return _count <= 0; });  // 只有当_count <= 0时，才停止等待
    }

    // 重置闭锁，允许再次使用
    void reset(int num_signal) {
        std::lock_guard<std::mutex> lock(_mutex);
        _count = num_signal;  // 重新设置需要的 Run 调用次数
    }

private:
    int _count;  // Run 需要达到的调用次数
    std::mutex _mutex;  // 保护 _count 和同步 wait 与 Run
    std::condition_variable _cv;  // 用于在 Run 被足够调用前阻塞 wait
};