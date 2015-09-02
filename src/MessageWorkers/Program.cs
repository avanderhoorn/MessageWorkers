using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading;
using System.Threading.Tasks;

namespace MessageWorkers
{
    public class Program
    {
        public void Main(string[] args)
        {

            var messageBroker = new MessageBroker();
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
                Console.WriteLine($"Off Req Observable    - Thread {Thread.CurrentThread.ManagedThreadId}     - \"{message.Description}\"");

                // QUESTION: This blocks until the minute is up from any other workers eating
                //           away at the queue, I would have thought that leveraging TaskPoolScheduler
                //           would mean that other workers/threads would pickup on the messages backingup 
                if (stall)
                {
                    stall = false;
                    Thread.Sleep(60000);
                }
            });



            messageGenerator.Start();

            Console.ReadLine();
        }
        
    }

    public class MessageBroker
    {
        private readonly ISubject<Message> _onRequestThreadSubject;
        private readonly ISubject<Message> _offRequestThreadSubject; 

        public MessageBroker()
        {
            _onRequestThreadSubject = new Subject<Message>();
            _offRequestThreadSubject = new Subject<Message>();

            // bring the data from the request thread onto a different thread
            // TODO: Instead of having this as one of the on thread subscribers, should spawn in SendMessage
            _onRequestThreadSubject.ObserveOn(TaskPoolScheduler.Default).Subscribe(message =>
            {
                _offRequestThreadSubject.OnNext(message);
            });
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
            // Question: Is this blocking? Given that MessageBroker is a signelton
            //           does it mean what only one message at a time can go through
            //           the observable pipeline
            // Tests: Current tests should that subscribers run on the same thread 
            //        that generated the message, this would imply that its ok?
            _onRequestThreadSubject.OnNext(message);
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
