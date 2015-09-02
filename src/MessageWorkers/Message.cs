using System;

namespace MessageWorkers
{
    public class Message
    {
        public Message(string description, int threadId)
        {
            Description = description;
            CreatedOnThreadId = threadId;
        }

        public int CreatedOnThreadId { get; }

        public Guid Id => Guid.NewGuid();

        public DateTime DateTime => DateTime.UtcNow;

        public string Description { get; }
    }
}