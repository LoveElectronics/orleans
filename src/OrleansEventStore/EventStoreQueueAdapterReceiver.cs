using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using Orleans.Runtime;
using Orleans.Streams;
using ServiceStack.MsgPack;

namespace Orleans.EventStore.Providers
{
    public class EventStoreQueueAdapterReceiver : IQueueAdapterReceiver
    {
        private readonly IEventStoreConnection m_EventStoreConnection;
        private readonly string m_StreamName;
        private readonly Logger m_Logger;

        private StreamMetadataResult m_Metadata;
        private EventStoreSubscription m_Subscription;
        private bool m_IsDropped;

        private ConcurrentQueue<ResolvedEvent> m_CachedEvents;

        public EventStoreQueueAdapterReceiver(IEventStoreConnection eventStoreConnection, string streamName, Logger logger)
        {
            m_EventStoreConnection = eventStoreConnection;
            m_StreamName = streamName;
            m_Logger = logger;

            m_CachedEvents = new ConcurrentQueue<ResolvedEvent>();
        }

        public Task Initialize(TimeSpan timeout)
        {
            CancellationTokenSource cancellationToken = new CancellationTokenSource(timeout);
            return Task.Run(async () =>
                {
                    m_Metadata = await m_EventStoreConnection.GetStreamMetadataAsync(m_StreamName);
                    await m_EventStoreConnection.SubscribeToStreamAsync(m_StreamName, true, EventAppeared, SubscriptionDropped);
                },
                cancellationToken.Token);
        }

        private void SubscriptionDropped(EventStoreSubscription eventStoreSubscription, SubscriptionDropReason subscriptionDropReason, Exception exception)
        {
            m_IsDropped = true;

            m_Logger.Info($"Subscription dropped, reason: {subscriptionDropReason}");
        }

        private void EventAppeared(EventStoreSubscription eventStoreSubscription, ResolvedEvent resolvedEvent)
        {
            m_CachedEvents.Enqueue(resolvedEvent);
        }

        public async Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
        {
            var batches = new List<IBatchContainer>();

            if (m_IsDropped || maxCount <= 0)
            {
                return batches;
            }
           
            var events = new List<ResolvedEvent>();

            int count = 0;
            ResolvedEvent @event;
            while (count < maxCount && m_CachedEvents.TryDequeue(out @event))
            {
                events.Add(@event);
                count++;
            }

            if (count == 0)
            {
                return batches;
            }

            batches.Add(new BatchContainer(Guid.Empty, m_StreamName, new EventStoreSequenceToken(events.First().Event.EventNumber), events.ToArray()));
            return batches;
        }

        public Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
        {
            throw new NotImplementedException();
        }

        public Task Shutdown(TimeSpan timeout)
        {
            if (m_Subscription != null)
            {
                m_Subscription.Close();
                m_Subscription.Dispose();
                m_Subscription = null;
            }
            return TaskDone.Done;
        }

        public class BatchContainer : IBatchContainer
        {
            private readonly ResolvedEvent[] m_ResolvedEvents;
            public Guid StreamGuid { get; private set; }
            public string StreamNamespace { get; private set; }
            public StreamSequenceToken SequenceToken { get; private set; }

            public BatchContainer(Guid streamGuid, string streamNamespace, StreamSequenceToken batchStartToken, ResolvedEvent[] resolvedEvents)
            {
                m_ResolvedEvents = resolvedEvents;
                StreamGuid = streamGuid;
                StreamNamespace = streamNamespace;
                SequenceToken = batchStartToken;
            }
            
            public IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>()
            {
                return m_ResolvedEvents.Select(x => new Tuple<T, StreamSequenceToken>(MsgPackExtensions.FromMsgPack<T>(x.Event.Data), new EventStoreSequenceToken(x.Event.EventNumber)));
            }

            public bool ImportRequestContext()
            {
                throw new NotImplementedException();
            }

            public bool ShouldDeliver(IStreamIdentity stream, object filterData, StreamFilterPredicate shouldReceiveFunc)
            {
                return true;
            }
        }
    }
}