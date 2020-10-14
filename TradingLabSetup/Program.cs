using KiteConnect;
using StackExchange.Redis;
using StackExchange.Redis.Extensions.Core.Abstractions;
using StackExchange.Redis.Extensions.Core.Configuration;
using StackExchange.Redis.Extensions.Core.Models;
using StackExchange.Redis.Extensions.Newtonsoft;
using System;
using System.Collections.Generic;
using System.Configuration;

namespace TradingLabSetup
{
    class Program
    {
        // instances of Kite and Ticker
        static Ticker ticker;
        static Kite kite;

        // Initialize key and secret of your app
        static string MyAPIKey = ConfigurationManager.AppSettings["MyAPIKey"];
        static string MySecret = ConfigurationManager.AppSettings["MySecret"];
        static string requestToken = ConfigurationManager.AppSettings["requestToken"]; // get it after login app

        // persist these data in settings or db or file
        static string MyPublicToken = "abcdefghijklmnopqrstuvwxyz";
        static string MyAccessToken = "abcdefghijklmnopqrstuvwxyz";

        // redis cache
        static NewtonsoftSerializer serializer = new NewtonsoftSerializer();
        static SinglePoolRedisCacheConnectionPoolManager spRedisCacheConnectionPoolMgr = new SinglePoolRedisCacheConnectionPoolManager(ConfigurationManager.AppSettings["redisConnectionString"]);
        static RedisConfiguration redisConfiguration = new RedisConfiguration();
        static IRedisDatabase cacheClient = new RedisCacheClient(spRedisCacheConnectionPoolMgr, serializer, redisConfiguration).Db0;

        static SortedSet<MyObject> inputList = new SortedSet<MyObject>(new MyObjectComparer());

        static void Main(string[] args)
        {

            kite = new Kite(MyAPIKey, Debug: true);

            // For handling 403 errors

            kite.SetSessionExpiryHook(OnTokenExpire);

            // Initializes the login flow

            try
            {
                initSession();

                kite.SetAccessToken(MyAccessToken);

                // Initialize ticker

                initTicker();
                Console.ReadKey();

                // Disconnect from ticker
                // ticker.Close();
            }
            catch (Exception e)
            {
                // Cannot continue without proper authentication
                Console.WriteLine(e.Message);
                //Console.ReadKey();
                Environment.Exit(0);
            }
        }

        private static void initSession()
        {
            //Console.WriteLine("Goto " + kite.GetLoginURL());
            //Console.WriteLine("Enter request token: ");
            //string requestToken = Console.ReadLine();
            User user = kite.GenerateSession(requestToken, MySecret);

            Console.WriteLine(Utils.JsonSerialize(user));

            MyAccessToken = user.AccessToken;
            MyPublicToken = user.PublicToken;
        }

        private static void initTicker()
        {
            ticker = new Ticker(MyAPIKey, MyAccessToken);

            ticker.OnTick += OnTick;
            ticker.OnReconnect += OnReconnect;
            ticker.OnNoReconnect += OnNoReconnect;
            ticker.OnError += OnError;
            ticker.OnClose += OnClose;
            ticker.OnConnect += OnConnect;
            ticker.OnOrderUpdate += OnOrderUpdate;

            ticker.EnableReconnect(Interval: 5, Retries: 50);
            ticker.Connect();

            // Subscribing to Banknifty and setting mode to full
            ticker.Subscribe(Tokens: new UInt32[] { 260105 });
            ticker.SetMode(Tokens: new UInt32[] { 260105 }, Mode: Constants.MODE_FULL);
        }

        private static void OnTokenExpire()
        {
            Console.WriteLine("Need to login again");
        }

        private static void OnConnect()
        {
            Console.WriteLine("Connected ticker");
        }

        private static void OnClose()
        {
            Console.WriteLine("Closed ticker");
        }

        private static void OnError(string Message)
        {
            Console.WriteLine("Error: " + Message);
        }

        private static void OnNoReconnect()
        {
            Console.WriteLine("Not reconnecting");
        }

        private static void OnReconnect()
        {
            Console.WriteLine("Reconnecting");
        }

        private static void OnTick(Tick TickData)
        {
            Console.WriteLine("Tick " + Utils.JsonSerialize(TickData));

            var latestTickData = new MyObject()
            {
                InstrumentID = 260105,
                Close = TickData.Close,
                High = TickData.High,
                Low = TickData.Low,
                Open = TickData.Open,
                TimeStamp = TickData.Timestamp.HasValue ? TickData.Timestamp.Value : DateTime.Now,
                Volume = TickData.Volume
            };

            // add data into cache
            //inputList.Add(latestTickData);

            //cacheClient.AddAsync("tickData", inputList);
        }

        private static void OnOrderUpdate(KiteConnect.Order OrderData)
        {
            Console.WriteLine("OrderUpdate " + Utils.JsonSerialize(OrderData));
        }
    }

    public class SinglePoolRedisCacheConnectionPoolManager : IRedisCacheConnectionPoolManager
    {
        private readonly string connectionString;

        public SinglePoolRedisCacheConnectionPoolManager(string connectionString)
        {
            this.connectionString = connectionString;
        }

        public void Dispose()
        {

        }

        public IConnectionMultiplexer GetConnection()
        {
            return ConnectionMultiplexer.Connect(connectionString);
        }

        public ConnectionPoolInformation GetConnectionInformations()
        {
            throw new NotImplementedException();
        }
    }

    public class MyObjectComparer : IComparer<MyObject>
    {
        public int Compare(MyObject x, MyObject y)
        {
            return x.TimeStamp.CompareTo(y.TimeStamp);
        }
    }
    public class MyObject
    {
        public uint InstrumentID { get; set; }
        public decimal Close { get; set; }
        public decimal High { get; set; }
        public decimal Low { get; set; }
        public decimal Open { get; set; }
        public DateTime TimeStamp { get; set; }
        public uint Volume { get; set; }

        public DateTime Created { get; } = DateTime.Now;
        public DateTime Ttl { get; } = DateTime.Now.AddMinutes(1);
        public DateTime? Persisted { get; set; }

        public bool IsDead => DateTime.Now > Ttl;
        public bool IsPersisted => Persisted.HasValue;
        public bool TimeToPersist => IsPersisted == false && DateTime.Now > Created.AddMinutes(1);

        public DateTime GetStartOfPeriodByMins(int numMinutes)
        {
            int oldMinutes = TimeStamp.Minute;
            int newMinutes = (oldMinutes / numMinutes) * numMinutes;

            DateTime startOfPeriod = new DateTime(TimeStamp.Year, TimeStamp.Month, TimeStamp.Day, TimeStamp.Hour, newMinutes, 0);

            return startOfPeriod;
        }
    }

}

