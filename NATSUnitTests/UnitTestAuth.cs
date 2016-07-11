﻿// Copyright 2015 Apcera Inc. All rights reserved.

using System;
using NATS.Client;
using System.Threading;
using System.Reflection;
using System.Linq;
using Xunit;

namespace NATSUnitTests
{
    /// <summary>
    /// Run these tests with the gnatsd auth.conf configuration file.
    /// </summary>
    public class TestAuthorization
    {
        int hitDisconnect;

        UnitTestUtilities util = new UnitTestUtilities();

        private void connectAndFail(String url)
        {
            try
            {
                System.Console.WriteLine("Trying: " + url);

                hitDisconnect = 0;
                Options opts = ConnectionFactory.GetDefaultOptions();
                opts.Url = url;
                opts.DisconnectedEventHandler += handleDisconnect;
                IConnection c = new ConnectionFactory().CreateConnection(url);

                throw new Exception("Expected a failure; did not receive one");
                
                c.Close();
            }
            catch (Exception e)
            {
                if (e.Message.Contains("Authorization"))
                {
                    System.Console.WriteLine("Success with expected failure: " + e.Message);
                }
                else
                {
                    throw new Exception("Unexpected exception thrown: " + e);
                }
            }
            finally
            {
                if (hitDisconnect > 0)
                    throw new Exception("The disconnect event handler was incorrectly invoked.");
            }
        }

        private void handleDisconnect(object sender, ConnEventArgs e)
        {
            hitDisconnect++;
        }

        [Fact]
        public void TestAuthSuccess()
        {
            using (NATSServer s = util.CreateServerWithConfig("auth_1222.conf"))
            {
                IConnection c = new ConnectionFactory().CreateConnection("nats://username:password@localhost:1222");
                c.Close();
            }
        }

        [Fact]
        public void TestAuthFailure()
        {
            try
            {
                using (NATSServer s = util.CreateServerWithConfig("auth_1222.conf"))
                {
                    connectAndFail("nats://username@localhost:1222");
                    connectAndFail("nats://username:badpass@localhost:1222");
                    connectAndFail("nats://localhost:1222");
                    connectAndFail("nats://badname:password@localhost:1222");
                }
            }
            catch (Exception e)
            {
                System.Console.WriteLine(e);
                throw e;
            }
        }

        [Fact]
        public void TestAuthToken()
        {
            try
            {
                using (NATSServer s = util.CreateServerWithArgs("-auth S3Cr3T0k3n!"))
                {
                    connectAndFail("nats://localhost:4222");
                    connectAndFail("nats://invalid_token@localhost:4222");

                    new ConnectionFactory().CreateConnection("nats://S3Cr3T0k3n!@localhost:4222").Close();
                }
            }
            catch (Exception e)
            {
                System.Console.WriteLine(e);
                throw e;
            }
        }


        [Fact]
        public void TestReconnectAuthTimeout()
        {
            ConditionalObj obj = new ConditionalObj();

            using (NATSServer s1 = util.CreateServerWithConfig("auth_1222.conf"),
                              s2 = util.CreateServerWithConfig("auth_1223_timeout.conf"),
                              s3 = util.CreateServerWithConfig( "auth_1224.conf"))
            {

                Options opts = ConnectionFactory.GetDefaultOptions();

                opts.Servers = new string[]{
                    "nats://username:password@localhost:1222",
                    "nats://username:password@localhost:1223", 
                    "nats://username:password@localhost:1224" };
                opts.NoRandomize = true;

                opts.ReconnectedEventHandler += (sender, args) =>
                {
                    obj.notify();
                };

                IConnection c = new ConnectionFactory().CreateConnection(opts);

                s1.Shutdown();

                // This should fail over to S2 where an authorization timeout occurs
                // then successfully reconnect to S3.

                obj.wait(20000);
            }
        }

        [Fact]
        public void TestReconnectAuthTimeoutLateClose()
        {
            ConditionalObj obj = new ConditionalObj();

            using (NATSServer s1 = util.CreateServerWithConfig("auth_1222.conf"),
                              s2 = util.CreateServerWithConfig("auth_1224.conf"))
            {

                Options opts = ConnectionFactory.GetDefaultOptions();

                opts.Servers = new string[]{
                    "nats://username:password@localhost:1222",
                    "nats://username:password@localhost:1224" };
                opts.NoRandomize = true;

                opts.ReconnectedEventHandler += (sender, args) =>
                {
                    obj.notify();
                };

                IConnection c = new ConnectionFactory().CreateConnection(opts);

                // inject an authorization timeout, as if it were processed by an incoming server message.
                // this is done at the parser level so that parsing is also tested,
                // therefore it needs reflection since Parser is an internal type.
                Type parserType = typeof(Connection).GetTypeInfo().Assembly.GetType("NATS.Client.Parser");
                Assert.True(parserType != null, "Failed to find NATS.Client.Parser");
                BindingFlags flags = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance;


                object parser = Activator.CreateInstance(parserType, c);
                Assert.True(parser != null, "Failed to instanciate a NATS.Client.Parser");
                MethodInfo parseMethod = parserType.GetMethod("parse", flags);
                Assert.True(parseMethod != null, "Failed to find method parse in NATS.Client.Parser");

                byte[] bytes = "-ERR 'Authorization Timeout'\r\n".ToCharArray().Select(ch => (byte)ch).ToArray();
                parseMethod.Invoke(parser, new object[] { bytes, bytes.Length });

                // sleep to allow the client to process the error, then shutdown the server.
                Thread.Sleep(250);
                s1.Shutdown();

                // Wait for a reconnect.
                obj.wait(20000);
            }
        }
    }
}
