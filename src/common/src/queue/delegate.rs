use super::Queue;

struct QueueDelegate {
    queue: dyn Queue,
}
