using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;

namespace MessageWorkers
{
    public class Program
    {
        public void Main(string[] args)
        {

            var messageBroker = new MessageBroker(new SlowMessagePublisher());
            var messageGenerator = new MessageGenerator(messageBroker);

            // On Request thread subscribers
            messageBroker.OnRequestThread.Subscribe(message =>
            {
                Console.WriteLine($"On Req Observable     - Thread {Thread.CurrentThread.ManagedThreadId}     - \"{message.Description}\"");
            });
            messageBroker.OnRequestThread.Subscribe(message =>
            {
                // Basic test to make sure that observable doesn't run only one message as once.
                Thread.Sleep(Thread.CurrentThread.ManagedThreadId % 2 == 0 ? 20000 : 50);
            });

            // Off Request thread subscribers
            var stall = true;
            messageBroker.OffRequestThread.Subscribe(message =>
            {
                Console.WriteLine($"Off Req Observable v  - Thread {Thread.CurrentThread.ManagedThreadId}     - \"{message.Description}\"");

                // QUESTION: This blocks until the minute is up from any other workers eating
                //           away at the queue, I would have thought that leveraging TaskPoolScheduler
                //           would mean that other workers/threads would pickup on the messages backingup 
                if (stall)
                {
                    stall = false;
                    Thread.Sleep(60000);
                }
                Console.WriteLine($"Off Req Observable ^  - Thread {Thread.CurrentThread.ManagedThreadId}     - \"{message.Description}\"");
            });
            
            messageGenerator.Start();

            Console.ReadLine();
        }
        
    }

    public class MessageBroker
    {
        private readonly ISubject<Message> _onRequestThreadSubject;
        private readonly ISubject<Message> _offRequestThreadSubject;
        private readonly ISubject<Message> _offRequestThreadInternalSubject;
        private readonly IMessagePublisher _messagePublisher;

        public MessageBroker(IMessagePublisher messagePublisher)
        {
            _messagePublisher = messagePublisher;

            _onRequestThreadSubject = new Subject<Message>();
            _offRequestThreadSubject = new Subject<Message>();
            _offRequestThreadInternalSubject = new Subject<Message>();

            // ensure off-request data is observed onto a different thread
            _offRequestThreadInternalSubject.Subscribe(x => Observable.Start(() => _offRequestThreadSubject.OnNext(x), TaskPoolScheduler.Default));
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
            _messagePublisher.Publish(message);

            // non-blocking
            _offRequestThreadInternalSubject.OnNext(message);

            // blocking
            _onRequestThreadSubject.OnNext(message);
        }
    }

    public interface IMessagePublisher
    {
        void Publish(Message message);
    }

    public class SlowMessagePublisher : IMessagePublisher
    {
        private readonly ISubject<Message> _listenerSubject;
        private int _count;

        public SlowMessagePublisher()
        {
            _listenerSubject = new Subject<Message>(); 


            // ensure off-request message transport is obsered onto a different thread 
            _listenerSubject.Buffer(TimeSpan.FromMilliseconds(10000)).Subscribe(x => Observable.Start(async () => await Publish(x), TaskPoolScheduler.Default)); 
        }

        public void Publish(Message message)
        {
            _listenerSubject.OnNext(message);
        }

        private async Task Publish(IEnumerable<Message> messages)
        {
            // Trying to simulate a slow activity i.e. HttpClient which has an await for sending over network
            await Task.Factory.StartNew(() =>
                {
                    foreach (var message in messages)
                    {
                        Console.WriteLine($"Publish Observable    - Thread {Thread.CurrentThread.ManagedThreadId}     - \"{message.Description}\" -  Group {_count}");
                    }
                    _count++;

                    Thread.Sleep(20000);
                });
        }
    }

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
                        //var sleep = Random.Next(1000, 2000);
                        var sleep = Random.Next(5000, 6000);
                        var description = $"Item {count++} - Thread {Thread.CurrentThread.ManagedThreadId} (sleep {sleep})";

                        Console.WriteLine($"Generated             - Thread {Thread.CurrentThread.ManagedThreadId}     - \"{description}\"");

                        MessageBroker.SendMessage(new Message(description));
                        Thread.Sleep(sleep);
                    }
                });
            }
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
