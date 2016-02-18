using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace OCSLogParser
{
    class UserEvent
    {
        private static int gSeq = 1000000;

        public ObjectId Id { get; set; }

        public int Seq { get; set; }
        public string EventName { get; set; }
        public string EventType { get; set; }
        public string FileName { get; set; }

        [BsonExtraElements]
        public Dictionary<string, object> Values { get; set; }

        public UserEvent()
        {
            this.Seq = ++gSeq;
            this.Values = new Dictionary<string, object>();
        }
    }

    class LogReader
    {
        public LogReader(LogParser parser)
        {
            this.parser = parser;
        }

        private LogParser parser;

        public void Read(System.IO.TextReader reader)
        {
            string line = string.Empty;
            while (reader.Peek() > 0)
            {
                line = reader.ReadLine();
                if (!string.IsNullOrEmpty(line))
                    this.parser.Parse(line);
            }
        }

        public void Finish()
        {
            this.parser.Finish();
        }
    }

    class LogParser{
        Regex eventExp = new Regex(@"^(?<eventType>received|request) .* message (?<eventName>\w+)$");
        Regex dataExp = new Regex(@"^\t{1,2}'{0,1}(?<key>\w+)'{0,1}\t(?<value>.*)$");

        private UserEvent currentEvent;

        Storage storage;

        public string FileName { get; set; }

        public LogParser(Storage storage)
        {
            this.storage = storage;
        }

        public void Parse(string line)
        {
            var eventMatch = eventExp.Match(line);
            var dataMatch = dataExp.Match(line);
            if (this.currentEvent != null && !dataMatch.Success)
            {
                this.StoreEvent();
                this.currentEvent = null;
            }
            if (eventMatch.Success)
            {
                this.currentEvent = new UserEvent();
                this.currentEvent.FileName = this.FileName;
                this.currentEvent.EventName = eventMatch.Groups["eventName"].Value;
                this.currentEvent.EventType = eventMatch.Groups["eventType"].Value;
            }
            if (this.currentEvent != null && dataMatch.Success)
            {
                object value = null;
                string key = dataMatch.Groups["key"].Value;
                value = this.ParseValue(dataMatch.Groups["value"].Value);
                this.currentEvent.Values.Add(key, value);
            }
        }

        private object ParseValue(string value)
        {
            int n = 0;
            if (int.TryParse(value, out n))
                return n;
            return value.Replace("'", "");
        }

        public void Finish()
        {
            if (this.currentEvent != null)
                this.StoreEvent();
            this.currentEvent = null;
        }

        private void StoreEvent()
        {
            if (this.currentEvent == null)
                return;
            this.storage.Store(this.currentEvent).Wait();
        }
    }

    class Storage
    {
        protected static IMongoClient _client;
        protected static IMongoDatabase _database;

        public static IMongoClient Client
        {
            get
            {
                if (_client == null)
                {
                    MongoDB.Driver.MongoClientSettings settings = new MongoClientSettings();
                    settings.Server = new MongoServerAddress("127.0.0.1", 27017);
                    settings.ConnectTimeout = TimeSpan.FromMinutes(5);
                    settings.SocketTimeout = TimeSpan.FromMinutes(5);
                    settings.MaxConnectionPoolSize = 30;
                    settings.MinConnectionPoolSize = 10;
                    settings.WaitQueueSize = 10000;
                    settings.WaitQueueTimeout = TimeSpan.FromMinutes(30);
                    _client = new MongoClient(settings);

                }
                return _client;
            }
        }

        public IMongoDatabase Database
        {
            get
            {
                if (_database == null)
                    _database = Client.GetDatabase(this.DatabaseName);
                return _database;
            }
        }

        public string DatabaseName { get; set; }
        public string CollectionName { get; set; }

        public async Task Store(UserEvent userEvent)
        {
            var collection = Database.GetCollection<UserEvent>(this.CollectionName);
            await collection.InsertOneAsync(userEvent);
        }
    }
}