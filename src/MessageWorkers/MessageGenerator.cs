using System;
using System.Threading;
using System.Threading.Tasks;

namespace MessageWorkers
{
    public class MessageGenerator
    {
        public MessageGenerator(MessageBroker messageBroker)
        {
            MessageBroker = messageBroker;
            Random = new Random();
        }

        private Random Random { get; }

        private MessageBroker MessageBroker { get; }

        public void Start()
        {
            var count = 0;
            for (var i = 0; i < 2; i++)
            {
                Task.Factory.StartNew(() =>
                {
                    while (true)
                    {
                        var sleep = Random.Next(1000, 2000);
                        //var sleep = Random.Next(5000, 6000);
                        var description = $"Item {count++} created on Thread {Thread.CurrentThread.ManagedThreadId} - (sleep {sleep})";

                        Console.ForegroundColor = ConsoleColor.Cyan;
                        Console.WriteLine($"[{Thread.CurrentThread.ManagedThreadId}] Generated - \"{description}\"");

                        var onRequestThread = sleep % 4 == 0; // run randomly on/off request thread (ca. 25% on request thread, 75% off request thread)

                        MessageBroker.SendMessage(new Message(description, Thread.CurrentThread.ManagedThreadId), onRequestThread);
                        Thread.Sleep(sleep);
                    }
                });
            }
        }
    }
}