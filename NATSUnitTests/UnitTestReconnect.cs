﻿// Copyright 2015 Apcera Inc. All rights reserved.

using System;
using NATS.Client;
using System.Text;
using System.Threading;
using System.Collections.Generic;
using Xunit;

namespace NATSUnitTests
{
    /// <summary>
    /// Run these tests with the gnatsd auth.conf configuration file.
    /// </summary>
    public class TestReconnect
    {

        private Options reconnectOptions = getReconnectOptions();

        private static Options getReconnectOptions()
        {
            Options o = ConnectionFactory.GetDefaultOptions();
            o.Url = "nats://localhost:22222";
            o.AllowReconnect = true;
            o.MaxReconnect = 10;
            o.ReconnectWait = 100;

            return o;
        }

        UnitTestUtilities utils = new UnitTestUtilities();

        public TestReconnect()
        {
            UnitTestUtilities.CleanupExistingServers();
        }

        [Fact]
        public void TestReconnectDisallowedFlags()
        {
            Options opts = ConnectionFactory.GetDefaultOptions();
            opts.Url = "nats://localhost:22222";
            opts.AllowReconnect = false;

            Object testLock = new Object();

            opts.ClosedEventHandler = (sender, args) =>
            {
                lock(testLock)
                {
                    Monitor.Pulse(testLock);
                }
            };

            using (NATSServer ns = utils.CreateServerOnPort(22222))
            {
                using (IConnection c = new ConnectionFactory().CreateConnection(opts))
                {
                    lock (testLock)
                    {
                        ns.Shutdown();
                        Assert.True(Monitor.Wait(testLock, 1000));
                    }
                }
            }
        }

        [Fact]
        public void TestReconnectAllowedFlags()
        {
            Options opts = ConnectionFactory.GetDefaultOptions();
            opts.Url = "nats://localhost:22222";
            opts.MaxReconnect = 2;
            opts.ReconnectWait = 1000;

            Object testLock = new Object();

            opts.ClosedEventHandler = (sender, args) =>
            {
                lock (testLock)
                {
                    Monitor.Pulse(testLock);
                }
            };

            using (NATSServer ns = utils.CreateServerOnPort(22222))
            {
                using (IConnection c = new ConnectionFactory().CreateConnection(opts))
                {
                    lock (testLock)
                    {
                        ns.Shutdown();
                        Assert.False(Monitor.Wait(testLock, 1000));
                    }

                    Assert.True(c.State == ConnState.RECONNECTING);
                    c.Opts.ClosedEventHandler = null;
                }
            }
        }

        [Fact]
        public void TestBasicReconnectFunctionality()
        {
            Options opts = ConnectionFactory.GetDefaultOptions();
            opts.Url = "nats://localhost:22222";
            opts.MaxReconnect = 2;
            opts.ReconnectWait = 1000;

            Object testLock = new Object();
            Object msgLock = new Object();

            opts.DisconnectedEventHandler = (sender, args) =>
            {
                lock (testLock)
                {
                    Monitor.Pulse(testLock);
                }
            };

            opts.ReconnectedEventHandler = (sender, args) =>
            {
                System.Console.WriteLine("Reconnected");
            };

            NATSServer ns = utils.CreateServerOnPort(22222);

            using (IConnection c = new ConnectionFactory().CreateConnection(opts))
            {
                IAsyncSubscription s = c.SubscribeAsync("foo");
                s.MessageHandler += (sender, args) =>
                {
                    System.Console.WriteLine("Received message.");
                    lock (msgLock)
                    {
                        Monitor.Pulse(msgLock);   
                    }
                };

                s.Start();
                c.Flush();

                lock (testLock)
                {
                    ns.Shutdown();
                    Assert.True(Monitor.Wait(testLock, 100000));
                }

                System.Console.WriteLine("Sending message.");
                c.Publish("foo", Encoding.UTF8.GetBytes("Hello"));
                System.Console.WriteLine("Done sending message.");
                // restart the server.
                using (ns = utils.CreateServerOnPort(22222))
                {
                    lock (msgLock)
                    {
                        c.Flush(50000);
                        Assert.True(Monitor.Wait(msgLock, 10000));
                    }

                    Assert.True(c.Stats.Reconnects == 1);
                }
            }
        }

        int received = 0;

        [Fact]
        public void TestExtendedReconnectFunctionality()
        {
            Options opts = reconnectOptions;

            Object disconnectedLock = new Object();
            Object msgLock = new Object();
            Object reconnectedLock = new Object();

            opts.DisconnectedEventHandler = (sender, args) =>
            {
                System.Console.WriteLine("Disconnected.");
                lock (disconnectedLock)
                {
                    Monitor.Pulse(disconnectedLock);
                }
            };

            opts.ReconnectedEventHandler = (sender, args) =>
            {
                System.Console.WriteLine("Reconnected.");
                lock (reconnectedLock)
                {
                    Monitor.Pulse(reconnectedLock);
                }
            };

            byte[] payload = Encoding.UTF8.GetBytes("bar");
            NATSServer ns = utils.CreateServerOnPort(22222);

            using (IConnection c = new ConnectionFactory().CreateConnection(opts))
            {
                IAsyncSubscription s1 = c.SubscribeAsync("foo");
                IAsyncSubscription s2 = c.SubscribeAsync("foobar");

                s1.MessageHandler += incrReceivedMessageHandler;
                s2.MessageHandler += incrReceivedMessageHandler;

                s1.Start();
                s2.Start();

                received = 0;

	            c.Publish("foo", payload);
                c.Flush();

                lock(disconnectedLock)
                {
                    ns.Shutdown();
                    // server is stopped here.

                    Assert.True(Monitor.Wait(disconnectedLock, 20000));
                }

                // subscribe to bar while connected.
                IAsyncSubscription s3 = c.SubscribeAsync("bar");
                s3.MessageHandler += incrReceivedMessageHandler;
                s3.Start();

                // Unsub foobar while disconnected
                s2.Unsubscribe();

                c.Publish("foo", payload);
                c.Publish("bar", payload);

                // server is restarted here...
                using (NATSServer ts = utils.CreateServerOnPort(22222))
                {
                    // wait for reconnect
                    lock (reconnectedLock)
                    {
                        Assert.True(Monitor.Wait(reconnectedLock, 60000));
                    }

                    c.Publish("foobar", payload);
                    c.Publish("foo", payload);

                    using (IAsyncSubscription s4 = c.SubscribeAsync("done"))
                    {
                        Object doneLock = new Object();
                        s4.MessageHandler += (sender, args) =>
                        {
                            System.Console.WriteLine("Recieved done message.");
                            lock (doneLock)
                            {
                                Monitor.Pulse(doneLock);
                            }
                        };

                        s4.Start();

                        lock (doneLock)
                        {
                            c.Publish("done", payload);
                            Assert.True(Monitor.Wait(doneLock, 2000));
                        }
                    }
                } // NATSServer

                Assert.True(received == 4, $"Expected 4, received {received}.");
            }
        }

        private void incrReceivedMessageHandler(object sender,
            MsgHandlerEventArgs args)
        {
            System.Console.WriteLine("Received message on subject {0}.",
                args.Message.Subject);
            Interlocked.Increment(ref received);
        }

        Dictionary<int, bool> results = new Dictionary<int, bool>();

        void checkResults(int numSent)
        {
            lock (results)
            {
                for (int i = 0; i < numSent; i++)
                {
                    Assert.True(results.ContainsKey(i), $"Received incorrect number of messages, {results[i]} for seq: {i}");
                }

                results.Clear();
            }
        }

        [Fact]
        public void TestClose()
        {
            Options opts = ConnectionFactory.GetDefaultOptions();
            opts.Url = "nats://localhost:22222";
            opts.AllowReconnect = true;
            opts.MaxReconnect = 60;

            using (NATSServer s1 = utils.CreateServerOnPort(22222))
            {
                IConnection c = new ConnectionFactory().CreateConnection(opts);
                Assert.False(c.IsClosed());
                
                s1.Shutdown();

                Thread.Sleep(100);
                Assert.True(c.IsReconnecting(), "Invalid state, expecting not closed, received: "
                        + c.State.ToString());
                
                using (NATSServer s2 = utils.CreateServerOnPort(22222))
                {
                    Thread.Sleep(1000);
                    Assert.False(c.IsClosed());
                
                    c.Close();
                    Assert.True(c.IsClosed());
                }
            }
        }

        [Fact]
        public void TestIsReconnectingAndStatus()
        {
            bool disconnected = false;
            object disconnectedLock = new object();

            bool reconnected = false;
            object reconnectedLock = new object();


            IConnection c = null;

            Options opts = ConnectionFactory.GetDefaultOptions();
            opts.Url = "nats://localhost:22222";
            opts.AllowReconnect = true;
            opts.MaxReconnect = 10000;
            opts.ReconnectWait = 100;

            opts.DisconnectedEventHandler += (sender, args) => 
            {
                lock (disconnectedLock)
                {
                    disconnected = true;
                    Monitor.Pulse(disconnectedLock);
                }
            };

            opts.ReconnectedEventHandler += (sender, args) => 
            {
                lock (reconnectedLock)
                {
                    reconnected = true;
                    Monitor.Pulse(reconnectedLock);
                }
            };

            using (NATSServer s = utils.CreateServerOnPort(22222))
            {
                c = new ConnectionFactory().CreateConnection(opts);

                Assert.True(c.State == ConnState.CONNECTED);
                Assert.True(c.IsReconnecting() == false);
            }
            // server stops here...

            lock (disconnectedLock)
            {
                if (!disconnected)
                    Assert.True(Monitor.Wait(disconnectedLock, 10000));
            }

            Assert.True(c.State == ConnState.RECONNECTING);
            Assert.True(c.IsReconnecting() == true);

            // restart the server
            using (NATSServer s = utils.CreateServerOnPort(22222))
            {
                lock (reconnectedLock)
                {
                    // may have reconnected, if not, wait
                    if (!reconnected)
                        Assert.True(Monitor.Wait(reconnectedLock, 10000));
                }

                Assert.True(c.IsReconnecting() == false);
                Assert.True(c.State == ConnState.CONNECTED);

                c.Close();
            }

            Assert.True(c.IsReconnecting() == false);
            Assert.True(c.State == ConnState.CLOSED);

        }


        [Fact]
        public void TestReconnectVerbose()
        {
            // an exception stops and fails the test.
            IConnection c = null;

            Object reconnectLock = new Object();
            bool   reconnected = false;

            Options opts = ConnectionFactory.GetDefaultOptions();
            opts.Verbose = true;

            opts.ReconnectedEventHandler += (sender, args) =>
            {
                lock (reconnectLock)
                {
                    reconnected = true;
                    Monitor.Pulse(reconnectLock);
                }
            };

            using (NATSServer s = utils.CreateServerOnPort(4222))
            {
                c = new ConnectionFactory().CreateConnection(opts);
                c.Flush();

                // exit the block and enter a new server block - this
                // restarts the server.
            }

            using (NATSServer s = utils.CreateServerOnPort(4222))
            {
                lock (reconnectLock)
                {
                    if (!reconnected)
                        Monitor.Wait(reconnectLock, 5000);
                }

                c.Flush();
            }
        }

    } // class

} // namespace
