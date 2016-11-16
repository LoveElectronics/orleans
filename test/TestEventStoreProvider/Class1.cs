using System;
using System.Collections.Generic;
using Orleans.EventStore.Providers;
using Orleans.Runtime.Configuration;
using Orleans.TestingHost;
using Tester.StreamingTests;
using Tester.TestStreamProviders.EventHub;
using TestExtensions;
using Xunit.Abstractions;


namespace TestEventStoreProvider
{
    [TestCategory("EventStore"), TestCategory("Streaming")]
    public class EHClientStreamTests : TestClusterPerTest
    {
        private const string StreamProviderName = "EventStoreStreamProvider";
        private const string StreamNamespace = "StreamNamespace";
        private const string EventStoreConnectionString = "ehorleanstest";

        private readonly ITestOutputHelper output;
        private readonly ClientStreamTestRunner runner;

        public EHClientStreamTests(ITestOutputHelper output)
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

        public override void Dispose()
        {
            base.Dispose();
        }

        private static void AdjustConfig(ClusterConfiguration config)
        {
            // register stream provider
            config.AddMemoryStorageProvider("PubSubStore");
            config.Globals.RegisterStreamProvider<TestEventHubStreamProvider>(StreamProviderName, BuildProviderSettings());
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
