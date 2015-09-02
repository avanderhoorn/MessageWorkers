using System;
using System.Threading;

namespace MessageWorkers
{
    public class Program
    {
        private readonly object _consoleLock = new object(); // locking console coloring :)

        public void Main(string[] args)
        {
            var messageBroker = new MessageBroker();
            var messageGenerator = new MessageGenerator(messageBroker);

            // On Request thread subscribers
            messageBroker.OnRequestThread.Subscribe(message =>
            {
                var subscriberThreadId = Thread.CurrentThread.ManagedThreadId;
                lock (_consoleLock)
                {
                    // ON Request means subscriber and observable must share thread id...
                    Console.ForegroundColor = subscriberThreadId == message.CreatedOnThreadId ? ConsoleColor.Yellow : ConsoleColor.Red;
                    Console.WriteLine($"[{subscriberThreadId}] ON Request processing - \"{message.Description}\"");
                }
            });

            // Off Request thread subscribers
            messageBroker.OffRequestThread.Subscribe(message =>
            {
                var subscriberThreadId = Thread.CurrentThread.ManagedThreadId;
                lock (_consoleLock)
                {
                    // OFF Request means subscriber and observable cannot share thread id...
                    Console.ForegroundColor = subscriberThreadId != message.CreatedOnThreadId ? ConsoleColor.White : ConsoleColor.Red;
                    Console.WriteLine($"[{subscriberThreadId}] OFF Request processing - \"{message.Description}\"");
                }
            });

            messageGenerator.Start();

            Console.ReadLine();
        }
    }
}
