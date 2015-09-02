using System;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Reactive.Subjects;

namespace MessageWorkers
{
    public class MessageBroker
    {
        private readonly ISubject<Message> _onRequestThreadSubject;
        private readonly ISubject<Message> _offRequestThreadSubject;
        private readonly IObservable<Message> _offRequestThreadObservable;
        private readonly IObservable<Message> _onRequestThreadObservable;

        public MessageBroker()
        {
            _onRequestThreadSubject = new Subject<Message>();
            _offRequestThreadSubject = new Subject<Message>();

            // ensure off-request data is observed onto a different thread
            var newThreadScheduler = new NewThreadScheduler();
            _offRequestThreadObservable = _offRequestThreadSubject.ObserveOn(newThreadScheduler);

            // ensure on-request data is observed onto the current thread (the request thread, assuming the messagebroker was created on the request thread :))
            _onRequestThreadObservable = _onRequestThreadSubject.SubscribeOn(CurrentThreadScheduler.Instance);
        }

        public IObservable<Message> OnRequestThread
        {
            get { return _onRequestThreadObservable; }
        }

        public IObservable<Message> OffRequestThread
        {
            get { return _offRequestThreadObservable; }
        }

        public void SendMessage(Message message, bool onRequestThread = false)
        {
            if (onRequestThread)
            {
                _onRequestThreadSubject.OnNext(message);
            }
            else
            {
                _offRequestThreadSubject.OnNext(message);
            }
        }
    }
}