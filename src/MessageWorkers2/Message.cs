using System;

namespace MessageWorkers2
{
    public class Message
    {
        public Message(string description)
        {
            Description = description;
        }

        public Guid Id => Guid.NewGuid();

        public DateTime DateTime => DateTime.Now;

        public string Description { get; }
    }
}
