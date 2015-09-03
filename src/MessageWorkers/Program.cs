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

    // OVERVIEW
    // Overall I am trying to optimize the processing of messages that occur
    // during the lifetime of an Asp.Net site. These messages are generated 
    // from indovidual requests that are being handeled by the server. Each 
    // request could generate 10 - 30 messages (exact numbers aren't known,
    // but for testing we are assuming higher than lower).
    //
    // Each message that is generated, eventually needs to find it way to our
    // server for processing. To help optimize the pipeline, we have designed 
    // the server so that order that the messages arive is not important. In 
    // addition to sending the data out of proc, we also want to allow extension
    // authors to tap the message stream if they desire.
    //
    // Overall we should be aiming to try and get off the orgin request as soon
    // as we can.
    //
    //GOALS
    // 1) On Request Thread Hook:
    //    Provide an extenion point that allows authors to have their code run
    //    INSIDE of the request context. This extenion point will be used with
    //    care, but there times when an author needs that context for additional
    //    information or to modify the request. 
    //
    //    It is expected that this will be blocking call (i.e. processing on 
    //    the request won't continue until all subscribers have finished).
    //
    // 2) Off Request Thread Hook:
    //    Provide an extenion point that allows authors to have their code run
    //    OUTSIDE of the request context. This extenion point will be used more
    //    commonly, as it doesn't directly hold up the request. As expected, this
    //    doesn't have access to the request context but does have access to the
    //    message and its payload.
    //
    //    It is expected that this will be a non blocking call (i.e. processing on 
    //    the request will continue as subscribers run in the background). Ideally, 
    //    as messages come in, they would queue to be processed and as needed, 
    //    one or many workers will pickup the messages for processing - running 
    //    through the registered subscrubers.
    //
    // 3) Off Request Thead Message Publishing
    //    As messages come into the broker, we should be trying to send those off 
    //    to the server as quickly as we can. This process shouldn't hold up the 
    //    request thread and should allow account different connection strategies
    //    to process their messages in different ways (i.e. slow connection 
    //    strategies may choose to buffer messages before sending the data out).

    public class Program
    {
        public void Main(string[] args)
        {

            var messageBroker = new MessageBroker(new SlowMessagePublisher());
            var messageGenerator = new MessageGenerator(messageBroker);

            // TEST: On Request 
            //     - Can register subscriber 
            //     - That they run on the same thread as the origin request 
            //     - One slow subscriber doesn't block other on request subscribers 
            messageBroker.OnRequestThread.Subscribe(message =>
            {
                Console.WriteLine($"On Req Observable v   - Thread {Thread.CurrentThread.ManagedThreadId}     - \"{message.Description}\"");

                Thread.Sleep(Thread.CurrentThread.ManagedThreadId % 2 == 0 ? 20000 : 50);

                Console.WriteLine($"On Req Observable ^   - Thread {Thread.CurrentThread.ManagedThreadId}     - \"{message.Description}\"");
            });

            // TEST: Off Request 
            //     - Can register subscriber 
            //     - That they run on a different thread as the origin request 
            //     - One slow subscriber doesn't block other off request subscribers 
            //     - When needed other workers are brought in to handel the load
            var stall = true;
            messageBroker.OffRequestThread.Subscribe(message =>
            {
                Console.WriteLine($"Off Req Observable v  - Thread {Thread.CurrentThread.ManagedThreadId}     - \"{message.Description}\"");
                
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
        private readonly ISubject<Message> _publisherInternalSubject;
        private readonly IMessagePublisher _messagePublisher;

        public MessageBroker(IMessagePublisher messagePublisher)
        {
            _messagePublisher = messagePublisher;

            _onRequestThreadSubject = new Subject<Message>();
            _offRequestThreadSubject = new Subject<Message>();
            _offRequestThreadInternalSubject = new Subject<Message>();
            _publisherInternalSubject = new Subject<Message>();

            // ensure off-request data is observed onto a different thread
            _offRequestThreadInternalSubject.Subscribe(x => Observable.Start(() => _offRequestThreadSubject.OnNext(x), TaskPoolScheduler.Default));
            _publisherInternalSubject.Subscribe(x => Observable.Start(() => _messagePublisher.Publish(x), TaskPoolScheduler.Default));
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
            // non-blocking ***
            _publisherInternalSubject.OnNext(message);

            // non-blocking
            _offRequestThreadInternalSubject.OnNext(message);

            // blocking
            _onRequestThreadSubject.OnNext(message);
        }

        // *** Question
        // Is the overhead of leveraging an observable just to get off 
        // the thead worth it or is there a better way of doing this?
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
