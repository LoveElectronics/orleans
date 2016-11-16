using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans.EventStore.Providers;
using Orleans.Runtime;
using Orleans.Runtime.Configuration;
using Orleans.TestingHost;
using Tester.StreamingTests;
using Tester.TestStreamProviders.EventHub;
using TestExtensions;
using Xunit;
using Xunit.Abstractions;


namespace TestEventStoreProvider
{
    [TestCategory("EventStore"), TestCategory("Streaming")]
    public class ESClientStreamTests : TestClusterPerTest
    {
        private const string StreamProviderName = "EventStoreStreamProvider";
        private const string StreamNamespace = "StreamNamespace";
        private const string EventStoreConnectionString = "ConnectTo=tcp://localhost:1113";

        private readonly ITestOutputHelper output;
        private readonly ClientStreamTestRunner runner;

        public ESClientStreamTests(ITestOutputHelper output)
        {
            this.output = output;
            runner = new ClientStreamTestRunner(this.HostedCluster);
        }

        public override TestCluster CreateTestCluster()
        {
            var options = new TestClusterOptions(2);
            AdjustConfig(options.ClusterConfiguration);
            AdjustConfig(options.ClientConfiguration);
            return new TestCluster(options);
        }

        [Fact]
        public async Task ESStreamProducerOnDroppedClientTest()
        {
            logger.Info("************************ EHStreamProducerOnDroppedClientTest *********************************");
            await runner.StreamProducerOnDroppedClientTest(StreamProviderName, StreamNamespace);
        }

        public override void Dispose()
        {
            base.Dispose();
        }

        private static void AdjustConfig(ClusterConfiguration config)
        {
            // register stream provider
            config.AddMemoryStorageProvider("PubSubStore");
            config.Globals.RegisterStreamProvider<EventStoreStreamProvider>(StreamProviderName, BuildProviderSettings());
            config.Globals.ClientDropTimeout = TimeSpan.FromSeconds(5);
        }

        private static void AdjustConfig(ClientConfiguration config)
        {
            config.RegisterStreamProvider<EventStoreStreamProvider>(StreamProviderName, BuildProviderSettings());
        }

        private static Dictionary<string, string> BuildProviderSettings()
        {
            var settings = new Dictionary<string, string>();
            settings.Add("ConnectionString", EventStoreConnectionString);
            return settings;
        }
    }
}
