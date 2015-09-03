using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading;
using System.Threading.Tasks;

namespace MessageWorkers2
{
    public class Program
    {
        public static int MessagesGenerated = 0;
        public static int MessagesSent = 0;
        public static bool Debug = false;

        public void Main(string[] args)
        {
            var messageBroker = new MessageBroker();
            var messageGenerator = new MessageGenerator(messageBroker);

            // On Request thread subscribers
            messageBroker.OnRequestThread.Subscribe(message =>
            {
                if (Debug)
                    Console.WriteLine($"On Req Observable v   - Thread {Thread.CurrentThread.ManagedThreadId}     - \"{message.Description}\"");

                // Simulate some amount of work.
                GC.KeepAlive("Hello".GetHashCode());

                if (Debug)
                    Console.WriteLine($"On Req Observable ^   - Thread {Thread.CurrentThread.ManagedThreadId}     - \"{message.Description}\"");
            });

            // Off Request thread subscribers
            messageBroker.OffRequestThread.Subscribe(message => Task.Run(async () =>
            {
                if (Debug)
                    Console.WriteLine($"Off Req Observable v  - Thread {Thread.CurrentThread.ManagedThreadId}     - \"{message.Description}\"");

                // Simulate some expensive async operation
                await Task.Delay(Thread.CurrentThread.ManagedThreadId % 2 == 0 ? 150 : 30);

                if (Debug)
                    Console.WriteLine($"Off Req Observable ^  - Thread {Thread.CurrentThread.ManagedThreadId}     - \"{message.Description}\"");
            }));

            var stopwatch = Stopwatch.StartNew();
            messageGenerator.Start();


            Console.WriteLine("Press the ANY key to get a summary.");
            Console.WriteLine("Press Ctrl+C to quit.");
            while (true)
            {
                Console.ReadLine();

                var elapsed = stopwatch.Elapsed;
                Console.WriteLine($"Messages Generated: {MessagesGenerated}");
                Console.WriteLine($"Messages Sent: {MessagesSent}");
                Console.WriteLine($"Time Elapsed: {elapsed}");
                Console.WriteLine($"Requests/sec: {(double)MessagesSent / ((double)elapsed.Ticks / (double)TimeSpan.FromSeconds(1).Ticks)}");
                Console.WriteLine($"Generated/sec: {(double)MessagesGenerated / ((double)elapsed.Ticks / (double)TimeSpan.FromSeconds(1).Ticks)}");
                Console.WriteLine();
                Console.WriteLine();
            }
        }
    }

    public class MessageBroker
    {
        private readonly ISubject<Message> _onRequestThreadSubject;
        private readonly ISubject<Message> _offRequestThreadSubject;
        private readonly ISubject<Message> _offRequestThreadInternalSubject;
        private readonly ClientPool _client;

        public MessageBroker()
        {
            _onRequestThreadSubject = new Subject<Message>();
            _offRequestThreadSubject = new Subject<Message>();
            _offRequestThreadInternalSubject = new Subject<Message>();

            // ensure off-request data is observed onto a different thread
            _offRequestThreadInternalSubject.Subscribe(x => Observable.Start(() => _offRequestThreadSubject.OnNext(x), TaskPoolScheduler.Default));

            _client = new ClientPool();
            _client.Start();
        }

        public IObservable<Message> OnRequestThread
        {
            get { return _onRequestThreadSubject; }
        }

        public IObservable<Message> OffRequestThread
        {
            get { return _offRequestThreadSubject; }
        }

        public void SendMessage(Message message)
        {
            // should be non-blocking but up to implementation 
            _client.Enqueue(message);

            // non-blocking
            _offRequestThreadInternalSubject.OnNext(message);

            // blocking
            _onRequestThreadSubject.OnNext(message);
        }
    }

    public class MessageGenerator
    {
        public MessageGenerator(MessageBroker messageBroker)
        {
            MessageBroker = messageBroker;
        }

        private MessageBroker MessageBroker { get; }

        public void Start()
        {
            for (var i = 0; i < 10; i++)
            {
                Task.Factory.StartNew(() =>
                {
                    var count = 0;
                    var seed = Thread.CurrentThread.ManagedThreadId;
                    var random = new Random(seed);

                    while (true)
                    {
                        var messageCount = random.Next(10, 20);

                        for (var j = 0; j < messageCount; j++)
                        {
                            MessageBroker.SendMessage(new Message($"Message - {seed}.{count++}"));
                            Interlocked.Increment(ref Program.MessagesGenerated);
                        }

                        Thread.Sleep(random.Next(50, 250));
                    }
                });
            }
        }
    }

    public class ClientPool : IObserver<Message>
    {
        private static readonly TimeSpan NetworkDelay = TimeSpan.FromSeconds(1.50);
        private static readonly TimeSpan CleanupFrequency = TimeSpan.FromMilliseconds(250);
        private static int ActiveReaderLimit = 50;
        private static int ActiveHttpLimit = 20;
        private const int MaxBatchSize = 100;

        private SemaphoreSlim ActiveReaderSemaphore { get; } = new SemaphoreSlim(ActiveReaderLimit, ActiveReaderLimit);
        private SemaphoreSlim HttpSemaphore { get; } = new SemaphoreSlim(ActiveHttpLimit, ActiveHttpLimit);

        private Timer CleanupTimer { get; set; }

        private ConcurrentQueue<Message> Queue { get; } = new ConcurrentQueue<Message>();

        public void Start()
        {
            CleanupTimer = new Timer(Timer_Tick, null, CleanupFrequency, CleanupFrequency);
        }

        public void Stop()
        {
            CleanupTimer.Dispose();
            CleanupTimer = null;
        }

        public void Enqueue(Message message)
        {
            Queue.Enqueue(message);

            if (Queue.Count > MaxBatchSize && ActiveReaderSemaphore.Wait(TimeSpan.FromMilliseconds(0)))
            {
                Task.Run(DrainQueue);
            }
        }

        private async Task DrainQueue()
        {
            var batch = new List<Message>();

            try
            {
                Message message;
                while (Queue.TryDequeue(out message) && batch.Count < MaxBatchSize)
                {
                    batch.Add(message);
                }

                if (batch.Count > 0)
                {
                    // Limit number of concurrent clients
                    await HttpSemaphore.WaitAsync();

                    try
                    {
                        // Simulate network IO
                        await Task.Delay((int)NetworkDelay.TotalMilliseconds);

                        Interlocked.Add(ref Program.MessagesSent, batch.Count);
                    }
                    finally
                    {
                        HttpSemaphore.Release();
                    }
                }
            }
            finally
            {
                ActiveReaderSemaphore.Release();
            }
        }

        private void Timer_Tick(object state)
        {
            var count = Queue.Count;
            var readers = ActiveReaderLimit - ActiveReaderSemaphore.CurrentCount;
            var clients = ActiveHttpLimit - HttpSemaphore.CurrentCount;
            Console.WriteLine($"Timer Ticked: {count} messages queued - {readers} Active Readers - {clients} Active HTTP Clients.");

            if (ActiveReaderSemaphore.Wait(TimeSpan.FromMilliseconds(0)))
            {
                Task.Run(DrainQueue);
            }
        }

        void IObserver<Message>.OnNext(Message value)
        {
            Enqueue(value);
        }

        void IObserver<Message>.OnError(Exception error)
        {
            // Do Nothing :-D
        }

        void IObserver<Message>.OnCompleted()
        {
            Stop();
        }
    }

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
