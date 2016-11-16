using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using MsgPack.Serialization;
using Orleans.Providers;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;
using ServiceStack.MsgPack;

namespace Orleans.EventStore.Providers
{
    public class EventStoreSequenceToken : StreamSequenceToken
    {
        public int EventNumber { get; private set; }

        public EventStoreSequenceToken(int eventNumber)
        {
            EventNumber = eventNumber;
        }

        public override bool Equals(StreamSequenceToken other)
        {
            return (other as EventStoreSequenceToken)?.EventNumber == EventNumber;
        }

        public override int CompareTo(StreamSequenceToken other)
        {
            return (other as EventStoreSequenceToken)?.EventNumber.CompareTo(EventNumber) ?? -1;
        }
    }

    public class EventStoreAdapterFactory : IQueueAdapterFactory, IQueueAdapter
    {
        private string m_ProviderName;
        private Logger m_Logger;

        private IQueueAdapterCache m_AdapterCache;
        private IStreamQueueMapper m_QueueMapper;

        private IEventStoreConnection m_EventStoreConnection;
        private bool m_IsConnected;

        private ConcurrentDictionary<QueueId, EventStoreQueueAdapterReceiver> m_Receivers;

        public string Name => m_ProviderName;
        public bool IsRewindable => true;
        public StreamProviderDirection Direction => StreamProviderDirection.ReadWrite;

        public void Init(IProviderConfiguration config, string providerName, Logger logger, IServiceProvider serviceProvider)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            if (string.IsNullOrWhiteSpace(providerName)) throw new ArgumentNullException(nameof(providerName));
            if (logger == null) throw new ArgumentNullException(nameof(logger));

            m_ProviderName = providerName;
            m_Logger = logger;

            if (!config.Properties.ContainsKey("ConnectionString"))
            {
                throw new InvalidOperationException("ConnectionString should be provided as an attribute of the StreamProvider configuration.");
            }
            var connectionString = config.Properties["ConnectionString"];
            m_EventStoreConnection = EventStoreConnection.Create(connectionString);

            m_QueueMapper = new HashRingBasedStreamQueueMapper(100, "EventStore");
            m_AdapterCache = new SimpleQueueAdapterCache(100, m_Logger);

            m_Receivers = new ConcurrentDictionary<QueueId, EventStoreQueueAdapterReceiver>();
        }

        public async Task<IQueueAdapter> CreateAdapter()
        {
            if (!m_IsConnected)
            {
                m_Logger.Info("EventStore Stream Provider begun connected.");

                await m_EventStoreConnection.ConnectAsync();
                m_IsConnected = true;

                m_Logger.Info("EventStore Stream Provider connected successfully.");
            }

            return this;
        }

        public IQueueAdapterCache GetQueueAdapterCache()
        {
            return m_AdapterCache;
        }

        public IStreamQueueMapper GetStreamQueueMapper()
        {
            return m_QueueMapper;
        }

        public async Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId)
        {
            return new StreamFailureHandler();
        }

        public async Task QueueMessageBatchAsync<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events, StreamSequenceToken token,
            Dictionary<string, object> requestContext)
        {
            var eventData = events.Select(x =>
            {
                var memoryStream = new MemoryStream();
                MessagePackSerializer.Get<T>().Pack(memoryStream, x);
                var data = new EventData(Guid.NewGuid(), streamGuid.ToString(), false, memoryStream.ToArray(), new byte[0]);
                memoryStream.Dispose();
                return data;
            }).ToArray();
            await m_EventStoreConnection.AppendToStreamAsync(streamNamespace, ExpectedVersion.Any, eventData);
        }

        public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
        {
            return m_Receivers.GetOrAdd(queueId, ConstructAdapterReceiver);
        }

        private EventStoreQueueAdapterReceiver ConstructAdapterReceiver(QueueId queueId)
        {
            var streamName = queueId.GetStringNamePrefix();
            return new EventStoreQueueAdapterReceiver(m_EventStoreConnection, streamName, m_Logger);
        }

        public class StreamFailureHandler : IStreamFailureHandler
        {
            public bool ShouldFaultSubsriptionOnError => false;

            public Task OnDeliveryFailure(GuidId subscriptionId, string streamProviderName, IStreamIdentity streamIdentity,
                StreamSequenceToken sequenceToken)
            {
                return TaskDone.Done;
            }

            public Task OnSubscriptionFailure(GuidId subscriptionId, string streamProviderName, IStreamIdentity streamIdentity,
                StreamSequenceToken sequenceToken)
            {
                return TaskDone.Done;
            }
        }
    }
}