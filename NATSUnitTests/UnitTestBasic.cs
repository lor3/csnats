﻿// Copyright 2015 Apcera Inc. All rights reserved.

using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NATS.Client;
using System.Diagnostics;
using Xunit;

namespace NATSUnitTests
{
    /// <summary>
    /// Run these tests with the gnatsd auth.conf configuration file.
    /// </summary>
    public class TestBasic : IDisposable
    {
        UnitTestUtilities utils = new UnitTestUtilities();

        public TestBasic()
        {
            UnitTestUtilities.CleanupExistingServers();
            utils.StartDefaultServer();
        }
        
        public void Dispose()
        {
            utils.StopDefaultServer();
        }

        [Fact]
        public void TestConnectedServer()
        {
            IConnection c = new ConnectionFactory().CreateConnection();
           
            string u = c.ConnectedUrl;
            
            if (string.IsNullOrWhiteSpace(u))
                throw new Exception($"Invalid connected url {u}.");
                
            if (!Defaults.Url.Equals(u))
                throw new Exception($"Invalid connected url {u}.");

            c.Close();
            u = c.ConnectedUrl;

            if (u != null)
                throw new Exception("Url is not null after connection is closed.");
        }

        [Fact]
        public void TestMultipleClose()
        {
            IConnection c = new ConnectionFactory().CreateConnection();
            
            Task[] tasks = new Task[10];

            for (int i = 0; i < 10; i++)
            {

                tasks[i] = new Task(() => { c.Close(); });
                tasks[i].Start();
            }

            Task.WaitAll(tasks);
        }

        [Fact]
        public void TestBadOptionTimeoutConnect()
        {
            Options opts = ConnectionFactory.GetDefaultOptions();

            try
            {
                opts.Timeout = -1;
                throw new Exception("Able to set invalid timeout.");
            }
            catch (Exception)
            {}   
        }

        [Fact]
        public void TestSimplePublish()
        {
            using (IConnection c = new ConnectionFactory().CreateConnection())
            {
                c.Publish("foo", Encoding.UTF8.GetBytes("Hello World!"));
            }
        }

        [Fact]
        public void TestSimplePublishNoData()
        {
            using (IConnection c = new ConnectionFactory().CreateConnection())
            {
                c.Publish("foo", null);
            }
        }



        private bool compare(byte[] p1, byte[] p2)
        {
            // null case
            if (p1 == p2)
                return true;

            if (p1.Length != p2.Length)
                return false;

            for (int i = 0; i < p2.Length; i++)
            {
                if (p1[i] != p2[i])
                    return false;
            }

            return true;
        }

        private bool compare(byte[] payload, Msg m)
        {
            return compare(payload, m.Data);
        }

        private bool compare(Msg a, Msg b)
        {
            if (a.Subject.Equals(b.Subject) == false)
                return false;

            if (a.Reply != null && a.Reply.Equals(b.Reply))
            {
                return false;
            }

            return compare(a.Data, b.Data);
        }

        readonly byte[] omsg = Encoding.UTF8.GetBytes("Hello World");
        readonly object mu = new Object();
        IAsyncSubscription asyncSub = null;
        Boolean received = false;

        [Fact]
        public void TestAsyncSubscribe()
        {
            using (IConnection c = new ConnectionFactory().CreateConnection())
            {
                using (IAsyncSubscription s = c.SubscribeAsync("foo"))
                {
                    asyncSub = s;
                    s.MessageHandler += CheckReceivedAndValidHandler;
                    s.Start();

                    lock (mu)
                    {
                        received = false;
                        c.Publish("foo", omsg);
                        c.Flush();
                        Monitor.Wait(mu, 30000);
                    }

                    if (!received)
                        throw new Exception("Did not receive message.");
                }
            }
        }

        private void CheckReceivedAndValidHandler(object sender, MsgHandlerEventArgs args)
        {
            System.Console.WriteLine("Received msg.");

            if (compare(args.Message.Data, omsg) == false)
                throw new Exception("Messages are not equal.");

            if (args.Message.ArrivalSubcription != asyncSub)
                throw new Exception("Subscriptions do not match.");

            lock (mu)
            {
                received = true;
                Monitor.Pulse(mu);
            }
        }

        [Fact]
        public void TestSyncSubscribe()
        {
            using (IConnection c = new ConnectionFactory().CreateConnection())
            {
                using (ISyncSubscription s = c.SubscribeSync("foo"))
                {
                    c.Publish("foo", omsg);
                    Msg m = s.NextMessage(1000);
                    if (compare(omsg, m) == false)
                        throw new Exception("Messages are not equal.");
                }
            }
        }

        [Fact]
        public void TestPubWithReply()
        {
            using (IConnection c = new ConnectionFactory().CreateConnection())
            {
                using (ISyncSubscription s = c.SubscribeSync("foo"))
                {
                    c.Publish("foo", "reply", omsg);
                    Msg m = s.NextMessage(1000);
                    if (compare(omsg, m) == false)
                        throw new Exception("Messages are not equal.");
                }
            }
        }

        [Fact]
        public void TestFlush()
        {
            using (IConnection c = new ConnectionFactory().CreateConnection())
            {
                using (ISyncSubscription s = c.SubscribeSync("foo"))
                {
                    c.Publish("foo", "reply", omsg);
                    c.Flush();
                }
            }
        }

        [Fact]
        public void TestQueueSubscriber()
        {
            using (IConnection c = new ConnectionFactory().CreateConnection())
            {
                using (ISyncSubscription s1 = c.SubscribeSync("foo", "bar"),
                                         s2 = c.SubscribeSync("foo", "bar"))
                {
                    c.Publish("foo", omsg);
                    c.Flush(1000);

                    if (s1.QueuedMessageCount + s2.QueuedMessageCount != 1)
                        throw new Exception("Invalid message count in queue.");

                    // Drain the messages.
                    try { s1.NextMessage(100); }
                    catch (NATSTimeoutException) { }

                    try { s2.NextMessage(100); }
                    catch (NATSTimeoutException) { }

                    int total = 1000;

                    for (int i = 0; i < 1000; i++)
                    {
                        c.Publish("foo", omsg);
                    }
                    c.Flush(1000);

                    Thread.Sleep(1000);
                    
                    int r1 = s1.QueuedMessageCount;
                    int r2 = s2.QueuedMessageCount;

                    if ((r1 + r2) != total)
                    {
                        throw new Exception($"Incorrect number of messages: {(r1 + r2)} vs {total}");
                    }

                    if (Math.Abs(r1 - r2) > (total * .15))
                    {
                        throw new Exception("Too much variance between {r1} and {r2}");
                    }
                }
            }
        }

        [Fact]
        public void TestReplyArg()
        {
            using (IConnection c = new ConnectionFactory().CreateConnection())
            {
                using (IAsyncSubscription s = c.SubscribeAsync("foo"))
                {
                    s.MessageHandler += ExpectedReplyHandler;
                    s.Start();

                    lock(mu)
                    {
                        received = false;
                        c.Publish("foo", "bar", null);
                        Monitor.Wait(mu, 5000);
                    }
                }
            }

            if (!received)
                throw new Exception("Message not received.");
        }

        private void ExpectedReplyHandler(object sender, MsgHandlerEventArgs args)
        {
            if ("bar".Equals(args.Message.Reply) == false)
                throw new Exception("Expected \"bar\", received: " + args.Message);

            lock(mu)
            {
                received = true;
                Monitor.Pulse(mu);
            }
        }

        [Fact]
        public void TestSyncReplyArg()
        {
            using (IConnection c = new ConnectionFactory().CreateConnection())
            {
                using (ISyncSubscription s = c.SubscribeSync("foo"))
                {
                    c.Publish("foo", "bar", null);
                    c.Flush(30000);

                    Msg m = s.NextMessage(1000);
                    if ("bar".Equals(m.Reply) == false)
                        throw new Exception("Expected \"bar\", received: " + m);
                }
            }
        }

        [Fact]
        public void TestUnsubscribe()
        {
            int count = 0;
            int max = 20;

            using (IConnection c = new ConnectionFactory().CreateConnection())
            {
                using (IAsyncSubscription s = c.SubscribeAsync("foo"))
                {
                    Boolean unsubscribed = false;
                    asyncSub = s;
                    //s.MessageHandler += UnsubscribeAfterCount;
                    s.MessageHandler += (sender, args) =>
                    {
                        count++;
                        System.Console.WriteLine("Count = {0}", count);
                        if (count == max)
                        {
                            asyncSub.Unsubscribe();
                            lock (mu)
                            {
                                unsubscribed = true;
                                Monitor.Pulse(mu);
                            }
                        }
                    };
                    s.Start();

                    max = 20;
                    for (int i = 0; i < max; i++)
                    {
                        c.Publish("foo", null, null);
                    }
                    Thread.Sleep(100);
                    c.Flush();

                    lock (mu)
                    {
                        if (!unsubscribed)
                        {
                            Monitor.Wait(mu, 5000);
                        }
                    }
                }

                if (count != max)
                    throw new Exception($"Received wrong # of messages after unsubscribe: {count} vs {max}");
            }
        }

        [Fact]
        public void TestDoubleUnsubscribe()
        {
            using (IConnection c = new ConnectionFactory().CreateConnection())
            {
                using (ISyncSubscription s = c.SubscribeSync("foo"))
                {
                    s.Unsubscribe();

                    try
                    {
                        s.Unsubscribe();
                        throw new Exception("No Exception thrown.");
                    }
                    catch (Exception e)
                    {
                        System.Console.WriteLine("Expected exception {0}: {1}",
                            e.GetType(), e.Message);
                    }
                }
            }
        }

        [Fact]
        public void TestRequestTimeout()
        {
            using (IConnection c = new ConnectionFactory().CreateConnection())
            {
                try
                {
                    c.Request("foo", null, 500);
                    throw new Exception("Expected an exception.");
                }
                catch (NATSTimeoutException) 
                {
                    Console.WriteLine("Received expected exception.");
                }
            }
        }

        [Fact]
        public void TestRequest()
        {
            using (IConnection c = new ConnectionFactory().CreateConnection())
            {
                using (IAsyncSubscription s = c.SubscribeAsync("foo"))
                {
                    byte[] response = Encoding.UTF8.GetBytes("I will help you.");

                    s.MessageHandler += (sender, args) =>
                    {
                        c.Publish(args.Message.Reply, response);
                        c.Flush();
                    };

                    s.Start();

                    Msg m = c.Request("foo", Encoding.UTF8.GetBytes("help."),
                        5000);

                    if (!compare(m.Data, response))
                    {
                        throw new Exception("Response isn't valid");
                    }
                }
            }
        }

        [Fact]
        public void TestRequestNoBody()
        {
            using (IConnection c = new ConnectionFactory().CreateConnection())
            {
                using (IAsyncSubscription s = c.SubscribeAsync("foo"))
                {
                    byte[] response = Encoding.UTF8.GetBytes("I will help you.");

                    s.MessageHandler += (sender, args) =>
                    {
                        c.Publish(args.Message.Reply, response);
                    };

                    s.Start();

                    Msg m = c.Request("foo", null, 50000);

                    if (!compare(m.Data, response))
                    {
                        throw new Exception("Response isn't valid");
                    }
                }
            }
        }

        class TestReplier
        {
            string replySubject;
            string id;
            int delay;
            private IConnection c;
            private Stopwatch sw;
            Random r = new Random();

            public TestReplier(IConnection c, int maxDelay, string id, string replySubject, Stopwatch sw)
            {
                // Save off our data, then carry on.
                this.c = c;
                this.delay = maxDelay;
                this.sw = sw;
                this.id = id;
                this.replySubject = replySubject;
            }

            public void process()
            {
                // delay the response to simulate a heavy workload and introduce
                // variability
                Thread.Sleep(r.Next( (delay/5), delay));
                c.Publish(replySubject, Encoding.UTF8.GetBytes("reply"));
                c.Flush();
            }
        }

        // This test method tests mulitiple overlapping requests across many
        // threads.  The responder simulates work, to introduce variablility
        // in the request timing.
        [Fact]
        public void TestRequestSafetyWithThreads()
        {
            int MAX_DELAY = 1000;
            int TEST_COUNT = 300;

            Stopwatch sw = new Stopwatch();
            byte[] response = Encoding.UTF8.GetBytes("reply");

            // ThreadPool.SetMinThreads(300, 300);

            using (IConnection c1 = new ConnectionFactory().CreateConnection(),
                               c2 = new ConnectionFactory().CreateConnection())
            {
                using (IAsyncSubscription s = c1.SubscribeAsync("foo", (sender, args) => {
                    // We cannot block this thread... so copy our data, and spawn a thread
                    // to handle a delay and responding.
                    TestReplier t = new TestReplier(c1, MAX_DELAY,
                        Encoding.UTF8.GetString(args.Message.Data), 
                        args.Message.Reply,
                        sw);
                    new Thread(() => { t.process(); }).Start();
                }))
                {
                    c1.Flush();

                    // use lower level threads over tasks here for predictibility
                    Thread[] threads = new Thread[TEST_COUNT];                  
                    Random r = new Random();

                    for (int i = 0; i < TEST_COUNT; i++)
                    {
                        threads[i] = new Thread((() =>
                        {
                            // randomly delay for a bit to test potential timing issues.
                            Thread.Sleep(r.Next(100, 500));
                            c2.Request("foo", null, MAX_DELAY * 2);
                        }));
                    }

                    // sleep for one second to allow the threads to initialize.
                    Thread.Sleep(1000);

                    sw.Start();

                    // start all of the threads at the same time.
                    for (int i = 0; i < TEST_COUNT; i++)
                    {
                        threads[i].Start();
                    }

                    // wait for every thread to stop.
                    for (int i = 0; i < TEST_COUNT; i++)
                    {
                        threads[i].Join();
                    }

                    sw.Stop();

                    // check that we didn't process the requests consecutively.
                    Assert.True(sw.ElapsedMilliseconds < (MAX_DELAY * 2));
                }
            }
        }

        // This test is a useful comparison in determining the difference
        // between threads (above) and tasks and performance.  In some
        // environments, the NATS client will fail here, but succeed in the 
        // comparable test using threads.
        // Do not automatically run, for comparison purposes and future dev.
        //[Fact]
        public void TestRequestSafetyWithTasks()
        {
            int MAX_DELAY = 1000;
            int TEST_COUNT = 300;

            // ThreadPool.SetMinThreads(300, 300);

            Stopwatch sw = new Stopwatch();
            byte[] response = Encoding.UTF8.GetBytes("reply");

            using (IConnection c1 = new ConnectionFactory().CreateConnection(),
                               c2 = new ConnectionFactory().CreateConnection())
            {
                // Try parallel requests and check the performance.
                using (IAsyncSubscription s = c1.SubscribeAsync("foo", (sender, args) =>
                {
                    // We cannot block this NATS thread... so copy our data, and spawn a thread
                    // to handle a delay and responding.
                    TestReplier t = new TestReplier(c1, MAX_DELAY,
                        Encoding.UTF8.GetString(args.Message.Data),
                        args.Message.Reply,
                        sw);
                    new Task(() => { t.process(); }).Start();
                }))
                {
                    c1.Flush();

                    // Depending on resources, Tasks can be queueud up for quite while.
                    Task[] tasks = new Task[TEST_COUNT];
                    Random r = new Random();

                    for (int i = 0; i < TEST_COUNT; i++)
                    {
                        tasks[i] = new Task((() =>
                        {
                            // randomly delay for a bit to test potential timing issues.
                            Thread.Sleep(r.Next(100, 500));
                            c2.Request("foo", null, MAX_DELAY * 2);
                        }));
                    }

                    // sleep for one second to allow the tasks to initialize.
                    Thread.Sleep(1000);

                    sw.Start();

                    // start all of the threads at the same time.
                    for (int i = 0; i < TEST_COUNT; i++)
                    {
                        tasks[i].Start();
                    }

                    Task.WaitAll(tasks);

                    sw.Stop();

                    System.Console.WriteLine("Test took {0} ms", sw.ElapsedMilliseconds);

                    // check that we didn't process the requests consecutively.
                    Assert.True(sw.ElapsedMilliseconds < (MAX_DELAY * 2));
                }
            }
        }


        [Fact]
        public void TestFlushInHandler()
        {
            using (IConnection c = new ConnectionFactory().CreateConnection())
            {
                using (IAsyncSubscription s = c.SubscribeAsync("foo"))
                {
                    byte[] response = Encoding.UTF8.GetBytes("I will help you.");

                    s.MessageHandler += (sender, args) =>
                    {
                        try
                        {
                            c.Flush();
                            System.Console.WriteLine("Success.");
                        }
                        catch (Exception e)
                        {
                            throw new Exception("Unexpected exception: " + e);
                        }

                        lock (mu)
                        {
                            Monitor.Pulse(mu);
                        }
                    };

                    s.Start();

                    lock (mu)
                    {
                        c.Publish("foo", Encoding.UTF8.GetBytes("Hello"));
                        Monitor.Wait(mu);
                    }
                }
            }
        }

        [Fact]
        public void TestReleaseFlush()
        {
            IConnection c = new ConnectionFactory().CreateConnection();

            for (int i = 0; i < 1000; i++)
            {
                c.Publish("foo", Encoding.UTF8.GetBytes("Hello"));
            }

            new Task(() => { c.Close(); }).Start();
            c.Flush();
        }

        [Fact]
        public void TestCloseAndDispose()
        {
            using (IConnection c = new ConnectionFactory().CreateConnection())
            {
                c.Close();
            }
        }

        [Fact]
        public void TestInbox()
        {
            using (IConnection c = new ConnectionFactory().CreateConnection())
            {
                string inbox = c.NewInbox();
                Assert.False(string.IsNullOrWhiteSpace(inbox));
                Assert.True(inbox.StartsWith("_INBOX."));
            }
        }

        [Fact]
        public void TestStats()
        {
            using (IConnection c = new ConnectionFactory().CreateConnection())
            {
                byte[] data = Encoding.UTF8.GetBytes("The quick brown fox jumped over the lazy dog");
                int iter = 10;

                for (int i = 0; i < iter; i++)
                {
                    c.Publish("foo", data);
                }
                c.Flush(1000);

                IStatistics stats = c.Stats;
                Assert.Equal(iter, stats.OutMsgs);
                Assert.Equal(iter * data.Length, stats.OutBytes);

                c.ResetStats();

                // Test both sync and async versions of subscribe.
                IAsyncSubscription s1 = c.SubscribeAsync("foo");
                s1.MessageHandler += (sender, arg) => { };
                s1.Start();

                ISyncSubscription s2 = c.SubscribeSync("foo");

                for (int i = 0; i < iter; i++)
                {
                    c.Publish("foo", data);
                }
                c.Flush(1000);

                stats = c.Stats;
                Assert.Equal(2 * iter, stats.InMsgs);
                Assert.Equal(2 * iter * data.Length, stats.InBytes);
            }
        }

        [Fact]
        public void TestRaceSafeStats()
        {
            using (IConnection c = new ConnectionFactory().CreateConnection())
            {

                new Task(() => { c.Publish("foo", null); }).Start();

                Thread.Sleep(1000);

                Assert.Equal(1, c.Stats.OutMsgs);
            }
        }

        [Fact]
        public void TestBadSubject()
        {
            using (IConnection c = new ConnectionFactory().CreateConnection())
            {
                bool exThrown = false;
                try
                {
                    c.Publish("", null);
                }
                catch (Exception e)
                {
                    if (e is NATSBadSubscriptionException)
                        exThrown = true;
                }
                Assert.True(exThrown);
            }
        }

        [Fact]
        public void TestLargeMessage()
        {
            using (IConnection c = new ConnectionFactory().CreateConnection())
            {
                int msgSize = 51200;
                byte[] msg = new byte[msgSize];

                for (int i = 0; i < msgSize; i++)
                    msg[i] = (byte)'A';

                msg[msgSize-1] = (byte)'Z';

                using (IAsyncSubscription s = c.SubscribeAsync("foo"))
                {
                    Object testLock = new Object();

                    s.MessageHandler += (sender, args) =>
                    {
                        lock(testLock)
                        {
                            Monitor.Pulse(testLock);
                        }
                        Assert.True(compare(msg, args.Message.Data));
                    };

                    s.Start();

                    c.Publish("foo", msg);
                    c.Flush(1000);

                    lock(testLock)
                    {
                        Monitor.Wait(testLock, 2000);
                    }
                }
            }
        }

        [Fact]
        public void TestSendAndRecv()
        {
            using (IConnection c = new ConnectionFactory().CreateConnection())
            {
                using (IAsyncSubscription s = c.SubscribeAsync("foo"))
                {
                    int received = 0;
                    int count = 1000;

                    s.MessageHandler += (sender, args) =>
                    {
                        Interlocked.Increment(ref received);
                    };

                    s.Start();

                    for (int i = 0; i < count; i++)
                    {
                        c.Publish("foo", null);
                    }
                    c.Flush();

                    Thread.Sleep(500);

                    if (received != count)
                    {
                        throw new Exception($"Received ({received}) != count ({count})");
                    }
                }
            }
        }


        [Fact]
        public void TestLargeSubjectAndReply()
        {
            using (IConnection c = new ConnectionFactory().CreateConnection())
            {
                String subject = "";
                for (int i = 0; i < 1024; i++)
                {
                    subject += "A";
                }

                String reply = "";
                for (int i = 0; i < 1024; i++)
                {
                    reply += "A";
                }

                using (IAsyncSubscription s = c.SubscribeAsync(subject))
                {
                    Object testLock = new Object();

                    s.MessageHandler += (sender, args) =>
                    {
                        if (!subject.Equals(args.Message.Subject))
                            throw new Exception("Invalid subject received.");

                        if (!reply.Equals(args.Message.Reply))
                            throw new Exception("Invalid subject received.");

                        lock (testLock)
                        {
                            Monitor.Pulse(testLock);
                        }
                    };

                    s.Start();

                    c.Publish(subject, reply, null);
                    c.Flush();

                    lock (testLock)
                    {
                        Assert.True(Monitor.Wait(testLock, 1000));
                    }
                }
            }
        }

        [Fact]
        public void TestAsyncSubHandlerAPI()
        {
            using (IConnection c = new ConnectionFactory().CreateConnection())
            {
                int received = 0;

                EventHandler<MsgHandlerEventArgs> h = (sender, args) =>
                {
                    Interlocked.Increment(ref received);
                };

                using (IAsyncSubscription s = c.SubscribeAsync("foo", h))
                {
                    c.Publish("foo", null);
                    c.Flush();
                    Thread.Sleep(500);
                }

                using (IAsyncSubscription s = c.SubscribeAsync("foo", "bar", h))
                {
                    c.Publish("foo", null);
                    c.Flush();
                    Thread.Sleep(500);
                }

                if (received != 2)
                {
                    throw new Exception($"Received ({received}) != 2");
                }
            }
        }

        [Fact]
        public void TestUrlArgument()
        {
            string url1 = NATS.Client.Defaults.Url;
            string url2 = "nats://localhost:4223";
            string url3 = "nats://localhost:4224";

            string urls = url1 + "," + url2 + "," + url3;
            IConnection c = new ConnectionFactory().CreateConnection(urls);
            Assert.True(c.Opts.Servers[0].Equals(url1));
            Assert.True(c.Opts.Servers[1].Equals(url2));
            Assert.True(c.Opts.Servers[2].Equals(url3));

            c.Close();

            urls = url1 + "    , " + url2 + "," + url3;
            c = new ConnectionFactory().CreateConnection(urls);
            Assert.True(c.Opts.Servers[0].Equals(url1));
            Assert.True(c.Opts.Servers[1].Equals(url2));
            Assert.True(c.Opts.Servers[2].Equals(url3));
            c.Close();

            try
            {
                urls = "  " + url1 + "    , " + url2 + ",";
                c = new ConnectionFactory().CreateConnection(urls);
                throw new Exception("Invalid url was not detected");
            }
            catch (Exception) { }

            c = new ConnectionFactory().CreateConnection(url1);
            c.Close();
        }

    } // class

} // namespace
