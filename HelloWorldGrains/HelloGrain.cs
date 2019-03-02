using System;
using System.Threading.Tasks;
using Eveneum;
using HelloWorld.Interfaces;
using Microsoft.Azure.Documents.Client;
using Orleans;

namespace HelloWorld.Grains
{
    /// <summary>
    /// Orleans grain implementation class HelloGrain.
    /// </summary>
    public class HelloGrain : Orleans.Grain, IHello
    {
        private readonly IEventStore EventStore;

        public HelloGrain()
        {
            var documentClient = new DocumentClient(new System.Uri("https://localhost:8081"), "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==");

            this.EventStore = new EventStore(documentClient, "EventStore", "Hello");
            this.EventStore.DeleteMode = DeleteMode.HardDelete;
        }

        private string EventStoreKey { get; set; }
        private ulong? Version { get; set; }

        private string LastGreeting { get; set; }
        private ulong GreetingCount { get; set; }

        public override async Task OnActivateAsync()
        {
            await base.OnActivateAsync();

            this.EventStoreKey = $"{this.GetType().Name}~{this.GetPrimaryKeyLong()}";

            var stream = await this.EventStore.ReadStream(this.EventStoreKey);

            if (stream.HasValue)
            {
                this.Version = stream.Value.Version;

                if (stream.Value.Snapshot.HasValue)
                {
                    var snapshot = stream.Value.Snapshot.Value.Data as Snapshot;

                    this.LastGreeting = snapshot.LastGreeting;
                    this.GreetingCount = snapshot.Count;
                }

                foreach (var @event in stream.Value.Events)
                    this.ApplyEvent(@event.Body as IEvent);
            }
        }

        public async Task<string> SayHello(string greeting)
        {
            var response = $"You said: '{greeting}', I say: Hello! (Last greeting: {this.LastGreeting}, Greeting Count: {this.GreetingCount})";

            await this.RaiseEvent(new GreetingReceived(greeting));

            return response;
        }

        private void ApplyEvent(IEvent @event)
        {
            dynamic me = this;
            me.Apply((dynamic)@event);
        }

        private async Task RaiseEvent(IEvent @event)
        {
            this.ApplyEvent(@event);

            await this.EventStore.WriteToStream(this.EventStoreKey, new[] { new EventData { Body = @event, Version = this.Version.GetValueOrDefault() + 1, Metadata = DateTime.UtcNow } }, this.Version);

            this.Version = this.Version.GetValueOrDefault() + 1;

            await this.EventStore.CreateSnapshot(this.EventStoreKey, this.Version.Value, new Snapshot(this.LastGreeting, this.GreetingCount), deleteOlderSnapshots: true);
        }

        private void Apply(GreetingReceived @event)
        {
            this.LastGreeting = @event.Greeting;
            ++this.GreetingCount;
        }
    }

    public interface IEvent { }

    class GreetingReceived : IEvent
    {
        public GreetingReceived(string greeting)
        {
            this.Greeting = greeting;
        }

        public string Greeting { get; private set; }
    }

    class Snapshot
    {
        public Snapshot(string lastGreeting, ulong count)
        {
            this.LastGreeting = lastGreeting;
            this.Count = count;
        }

        public string LastGreeting { get; private set; }
        public ulong Count { get; private set; }
    }
}